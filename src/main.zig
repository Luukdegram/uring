const std = @import("std");
const Allocator = std.mem.Allocator;
const os = std.os;
const log = std.log.scoped(.server);

pub fn main() anyerror!void {
    std.log.info("All your codebase are belong to us.", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (@import("builtin").mode == .Debug) {
        std.debug.assert(!gpa.deinit());
    };
    var server = try Server.init(gpa.allocator(), 32, 0);
    try server.listen(try std.net.Address.parseIp("0.0.0.0", 8080));
    try server.run();
}

const Server = struct {
    ring: os.linux.IO_Uring,
    socket: os.socket_t,
    gpa: Allocator,

    /// Initializes a new `Server` instance as well as an `IO_Uring` instance.
    /// `entries` must be a power of two, between 1 and 4049.
    pub fn init(gpa: Allocator, entries: u13, flags: u32) !Server {
        return Server{
            .ring = try os.linux.IO_Uring.init(entries, flags),
            .gpa = gpa,
            .socket = undefined,
        };
    }

    /// Creates a new socket for the server to listen on, binds it to the given
    /// `address` and finally starts listening to new connections.
    pub fn listen(self: *Server, address: std.net.Address) !void {
        self.socket = try os.socket(
            address.any.family,
            os.SOCK.STREAM | os.SOCK.CLOEXEC,
            0,
        );

        try os.setsockopt(
            self.socket,
            os.SOL.SOCKET,
            os.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        );
        const socklen = address.getOsSockLen();
        try os.bind(self.socket, &address.any, socklen);
        try os.listen(self.socket, 10);
    }

    fn accept_client(self: *Server) !void {
        const request = try self.gpa.create(Client);
        request.* = .{
            .buffer = undefined,
            .event_type = .accept,
            .socket = undefined,
        };
        var sock_addr: os.sockaddr = undefined;
        var sock_len: os.socklen_t = @sizeOf(os.sockaddr);
        var sqe = try self.ring.accept(@ptrToInt(request), self.socket, &sock_addr, &sock_len, 0);
        _ = sqe; // TODO: What the heck do we do with this sqe?
        std.debug.assert(1 == try self.ring.submit());
    }

    pub fn run(self: *Server) !void {
        try self.accept_client();
        var completed_buffer: [128]os.linux.io_uring_cqe = undefined;
        while (true) {
            const count = try self.ring.copy_cqes(&completed_buffer, 0);
            if (count == 0) return;
            for (completed_buffer[0..count]) |cqe| {
                const client = Client.from_ptr(cqe.user_data);

                switch (cqe.err()) {
                    .SUCCESS => {},
                    else => |err| {
                        log.err("Request failed with error '{s}' for event: {s}", .{ @tagName(client.event_type), @tagName(err) });
                        client.deinit(self.gpa);
                        return;
                    },
                }
                switch (client.event_type) {
                    .accept => {
                        client.socket = cqe.res;
                        try client.recv(&self.ring);
                    },
                    .read => {
                        const len = @intCast(usize, cqe.res);
                        if (len == 0) {
                            log.info("Client disconnected, closing connection", .{});
                            return;
                        }
                        try client.read(&self.ring, len);
                    },
                    .write => client.deinit(self.gpa),
                }
            }
        }
    }
};

const Client = struct {
    /// Internal read buffer of the client
    buffer: [4096]u8,
    /// State of the client
    event_type: enum {
        accept,
        read,
        write,
    },
    /// File descriptor of Client's connection
    socket: os.socket_t,
    connected: bool = false,

    fn recv(self: *Client, ring: *os.linux.IO_Uring) !void {
        self.event_type = .read;
        _ = try ring.recv(self.as_ptr(), self.socket, &self.buffer, 0);
        std.debug.assert(1 == try ring.submit());
    }

    fn as_ptr(self: *Client) u64 {
        return @ptrToInt(self);
    }

    fn from_ptr(ptr: u64) *Client {
        return @intToPtr(*Client, ptr);
    }

    fn read(self: *Client, ring: *os.linux.IO_Uring, len: usize) !void {
        std.debug.assert(len < self.buffer.len);
        const content = self.buffer[0..len];
        log.info("Request: --------\n{s}", .{content});

        try self.send(ring);
    }

    fn send(self: *Client, ring: *os.linux.IO_Uring) !void {
        self.event_type = .write;
        _ = try ring.send(self.as_ptr(), self.socket, "HTTP/1.1 200 OK\r\nContent-length:11\r\n\r\nHello World", 0);
        std.debug.assert(1 == try ring.submit());
    }

    fn deinit(self: *Client, gpa: Allocator) void {
        if (self.connected) os.close(self.socket);
        gpa.destroy(self);
    }

    fn close(self: *Client, ring: *os.linux.IO_Uring) !void {
        try ring.close(self.as_ptr(), self.socket);
        std.debug.assert(1 == try ring.submit());
    }
};
