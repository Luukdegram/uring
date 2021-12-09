#Uring

Some fun experiments using io-uring to explore ideas.
Most of the ideas consist around the http protocol to see if I can come up with a viable replacement
for all IO currently used in [apple_pie](https://github.com/Luukdegram/apple_pie).

The code in here mostly implements happy-paths, and makes assumptions. Therefore it's not recommended to use
any of this as-is.
