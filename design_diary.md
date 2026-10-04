# Design Diary — NetMessenger

##
- Set up project structure and personalised directories.
- Decided to use the `epoll` concurrency model for better scalability with multiple clients.
- Planned data structures: `struct client` to hold fd, username, and current room. `struct room` to hold room name and members.
- Next step: Implement the server socket setup and the REGISTER command.
