const std = @import("std");
const Allocator = std.mem.Allocator;
const os = std.os;
const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const log = std.log.scoped(.server);

pub fn main() anyerror!void {
    std.log.info("All your codebase are belong to us.", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (@import("builtin").mode == .Debug) {
        std.debug.assert(!gpa.deinit());
    };
    var server = try Server.init(gpa.allocator(), 512, 0);
    try server.listen(try std.net.Address.parseIp("0.0.0.0", 8080));
    try server.run();
}

const Ring = struct {
    io: IO_Uring,
    first: ?*Completion = null,
    last: ?*Completion = null,

    /// Represents a completed event
    const Completion = struct {
        /// Result, context dependent and should always
        /// be verified by checking errNo()
        result: i32 = undefined,
        /// The next completion in line
        next: ?*Completion = null,
        /// The operation that was/is queued
        op: Op,
        /// Erased pointer to a context type
        context: ?*anyopaque,
        /// The callback to call upon completion
        callback: fn (completion: *Completion, result: anyerror!i32) void,

        fn from_addr(ptr: u64) *Completion {
            return @intToPtr(*Completion, ptr);
        }

        fn as_addr(self: *Completion) u64 {
            return @ptrToInt(self);
        }

        fn get_context(self: *Completion, comptime T: type) T {
            return @ptrCast(T, @alignCast(@alignOf(T), self.context));
        }
    };

    /// Defines an operator to be queued/completed.
    /// The max size of each tag is 28 bytes
    ///
    /// TODO: See if we can somehow lower those 28 bytes
    const Op = union(enum) {
        accept: struct {
            socket: i32,
            address: linux.sockaddr,
            address_len: linux.socklen_t,
            flags: u32,
        },
        recv: struct {
            socket: i32,
            buffer: []u8,
            flags: u32,
        },
        send: struct {
            socket: i32,
            buffer: []const u8,
            flags: u32,
        },
    };

    /// Ensures we get a new queue submission,
    /// by flushing the completion queue and submitting all current
    /// submittions when we failed to get a free submittion entry.
    fn get_sqe(self: *Ring) !*linux.io_uring_sqe {
        while (true) {
            return self.io.get_sqe() catch {
                try self.flush();
                _ = try self.io.submit();
                continue;
            };
        }
    }

    /// Copies all completed queue entries and appends
    /// each `Completion` to the intrusive linked list, allowing us
    /// to trigger those first when polling, rather than asking the
    /// kernel for more completions.
    fn flush(self: *Ring) !void {
        var queue: [256]linux.io_uring_cqe = undefined;
        while (true) {
            const found = try self.io.copy_cqes(&queue, 0);
            if (found == 0) {
                return; // no completions
            }
            // For each completed entry, add them to our linked-list
            for (queue[0..found]) |cqe| {
                const completed = Completion.from_addr(cqe.user_data);
                completed.result = cqe.res;

                if (self.first == null) {
                    self.first = completed;
                }
                if (self.last) |last| {
                    last.next = completed;
                }
                completed.next = null;
                self.last = completed;
            }
        }
    }

    fn submit(self: *Ring, completion: *Completion) !void {
        const sqe = try self.get_sqe();
        switch (completion.op) {
            .accept => |*op| linux.io_uring_prep_accept(
                sqe,
                op.socket,
                &op.address,
                &op.address_len,
                op.flags,
            ),
            .recv => |op| linux.io_uring_prep_recv(
                sqe,
                op.socket,
                op.buffer,
                op.flags,
            ),
            .send => |op| linux.io_uring_prep_send(
                sqe,
                op.socket,
                op.buffer,
                op.flags,
            ),
        }
        sqe.user_data = completion.as_addr();
    }

    fn err(maybe_err_no: i32) linux.E {
        return if (maybe_err_no > -4096 and maybe_err_no < 0)
            @intToEnum(linux.E, -maybe_err_no)
        else
            .SUCCESS;
    }

    fn complete(self: *Ring, completion: *Ring.Completion) !void {
        switch (completion.op) {
            .accept => {
                const result = switch (err(completion.result)) {
                    .SUCCESS => completion.result,
                    .INTR, .AGAIN => {
                        try self.submit(completion);
                        return;
                    },
                    else => |err_no| return os.unexpectedErrno(err_no),
                };
                completion.callback(completion, result);
            },
            .recv => {
                const result = switch (err(completion.result)) {
                    .SUCCESS => completion.result,
                    .INTR => {
                        try self.submit(completion);
                        return;
                    },
                    .CONNRESET => error.PeerResetConnection,
                    else => |err_no| return os.unexpectedErrno(err_no),
                };
                completion.callback(completion, result);
            },
            .send => {
                const result = switch (err(completion.result)) {
                    .SUCCESS => completion.result,
                    .INTR => {
                        try self.submit(completion);
                        return;
                    },
                    .CONNRESET => error.PeerResetConnection,
                    .PIPE => error.BrokenPipe,
                    else => |err_no| return os.unexpectedErrno(err_no),
                };
                completion.callback(completion, result);
            },
        }
    }

    /// For all completed entries, calls its callback.
    /// Then, submits all entries and waits until at least 1 entry has been submitted.
    /// Finally, copies all completed entries (if any) and stores them in a linked list for later
    /// consumptions.
    fn tick(self: *Ring) !void {
        while (self.first) |completion| {
            self.first = completion.next;
            if (self.first == null) {
                self.last = null;
            }
            try self.complete(completion);
        }

        _ = try self.io.submit_and_wait(1);
        try self.flush();
    }
};

const Server = struct {
    clients: [128]Client,
    free_client_slots: std.ArrayListUnmanaged(u7) = .{},
    gpa: Allocator,
    ring: Ring,
    socket: os.socket_t,
    completion: Ring.Completion,

    /// Initializes a new `Server` instance as well as an `IO_Uring` instance.
    /// `entries` must be a power of two, between 1 and 4049.
    pub fn init(gpa: Allocator, entries: u13, flags: u32) !Server {
        return Server{
            .ring = .{ .io = try os.linux.IO_Uring.init(entries, flags) },
            .clients = undefined,
            .gpa = gpa,
            .socket = undefined,
            .completion = undefined,
        };
    }

    fn setup_free_slots(self: *Server) void {
        for (self.clients) |_, index| {
            self.free_client_slots.appendAssumeCapacity(@intCast(u7, index));
        }
    }

    fn stop(self: *Server) void {
        _ = self;
        @panic("TODO");
    }

    /// Appends a new submission to accept a new client connection
    fn accept(self: *Server) !void {
        const accept_callback = struct {
            fn callback(completion: *Ring.Completion, result: anyerror!i32) void {
                _ = result catch {};
                const server = completion.get_context(*Server);
                defer server.accept() catch |err| {
                    log.err("Unexpected error: {s}, shutting down", .{@errorName(err)});
                    server.stop();
                };
                const socket = completion.result;
                const index = server.free_client_slots.pop();
                Client.init(&server.clients[index], index, socket, &server.ring) catch |err| {
                    log.warn("Failed to initialize client: {s}", .{@errorName(err)});
                    os.close(socket);
                    server.free_client_slots.appendAssumeCapacity(index);
                    return;
                };
            }
        }.callback;

        self.completion = .{
            .context = self,
            .callback = accept_callback,
            .op = .{
                .accept = .{
                    .socket = self.socket,
                    .address = undefined,
                    .address_len = @sizeOf(linux.sockaddr),
                    .flags = os.SOCK.NONBLOCK | os.SOCK.CLOEXEC,
                },
            },
        };
        try self.ring.submit(&self.completion);
    }

    /// Creates a new socket for the server to listen on, binds it to the given
    /// `address` and finally starts listening to new connections.
    pub fn listen(self: *Server, address: std.net.Address) !void {
        self.socket = try os.socket(
            os.AF.INET,
            os.SOCK.STREAM | os.SOCK.NONBLOCK | os.SOCK.CLOEXEC,
            os.IPPROTO.TCP,
        );
        errdefer os.close(self.socket);

        try os.setsockopt(
            self.socket,
            os.SOL.SOCKET,
            os.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        );
        const socklen = address.getOsSockLen();
        try os.bind(self.socket, &address.any, socklen);
        try os.listen(self.socket, 128);
    }

    /// Starts accepting new connections and initializes
    /// new clients for those, to read requests and send responses.
    pub fn run(self: *Server) !void {
        try self.free_client_slots.ensureTotalCapacity(self.gpa, 128);
        defer self.free_client_slots.deinit(self.gpa);

        self.setup_free_slots();
        try self.accept();
        while (true) {
            try self.ring.tick();
        }
    }
};

const Client = struct {
    /// Client ID assigned by the server,
    /// It is not safe to store this information as new connections
    /// may be assigned this ID when this Client has disconnected.
    index: u7,
    /// The file descriptor of this Client connection.
    socket: i32,
    /// Pointer to the io-uring object, allowing us to queue
    /// submissions to perform syscalls such as reads and writes.
    ring: *Ring,
    /// Frame of the client
    frame: @Frame(run),

    pub fn init(self: *Client, index: u7, socket: i32, ring: *Ring) !void {
        try std.os.setsockopt(socket, 6, os.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1)));

        self.index = index;
        self.socket = socket;
        self.ring = ring;

        self.frame = async self.run();
    }

    fn run(self: *Client) !void {
        while (true) {
            var buf: [4096]u8 = undefined;
            try self.recv(&buf);
            try self.send(HTTP_RESPONSE);
        }
    }

    fn recv(self: *Client, buffer: []u8) !void {
        var completion: Ring.Completion = .{
            .context = @frame(),
            .callback = recv_callback,
            .op = .{ .recv = .{
                .buffer = buffer,
                .socket = self.socket,
                .flags = os.linux.SOCK.CLOEXEC | os.linux.SOCK.NONBLOCK,
            } },
        };
        suspend try self.ring.submit(&completion);
    }

    fn recv_callback(completion: *Ring.Completion, result: anyerror!i32) void {
        resume completion.get_context(anyframe);
        _ = result catch return;
    }

    fn send(self: *Client, buffer: []const u8) !void {
        var completion: Ring.Completion = .{
            .context = @frame(),
            .callback = send_callback,
            .op = .{
                .send = .{
                    .buffer = buffer,
                    .socket = self.socket,
                    .flags = os.linux.SOCK.CLOEXEC | os.linux.SOCK.NONBLOCK,
                },
            },
        };
        suspend try self.ring.submit(&completion);
    }

    fn send_callback(completion: *Ring.Completion, result: anyerror!i32) void {
        resume completion.get_context(anyframe);
        _ = result catch return;
    }
};

const HTTP_RESPONSE =
    "HTTP/1.1 200 Ok\r\n" ++
    "Content-Length: 11\r\n" ++
    "Content-Type: text/plain; charset=utf8\r\n" ++
    "Date: Thu, 19 Nov 2021 15:26:34 GMT\r\n" ++
    "Server: uring-example\r\n" ++
    "\r\n" ++
    "Hello World";
