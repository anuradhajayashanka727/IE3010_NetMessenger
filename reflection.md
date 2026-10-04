# Structured Reflection — IE3010 NetMessenger

Registration Number: IT23695634  
Name: Anuradha Jayashanka



## 1. AI Tools Used and Where

I used ChatGPT throughout the project to explain TCP concepts, review my code, and debug compiler warnings. It helped most with TCP stream framing, exact-byte file reception, and interpreting GCC `-Wformat-truncation` warnings.

## 2. What AI Did Well

The most useful explanation was that TCP is a byte stream, not a message stream, so framing is entirely the application's responsibility. AI also helped me diagnose why my server crashed whenever a client disconnected (`SIGPIPE`) and how to fix it with `MSG_NOSIGNAL`.

## 3. Where AI Was Wrong

AI suggestions still required local testing. An early `sscanf` parser broke on multi-word messages like `BCAST Hello World`. My first file-transfer implementation silently corrupted files because the client called `recv()` again instead of consuming the header plus file bytes already sitting in its buffer. I only caught this by running `diff` on received files.

## 4. What I Changed or Rejected

I rejected AI's simpler file handling and wrote my own `recv_exact_buffered()`, token walker, buffer-draining logic, and input validation. I fixed truncation warnings by bounding inputs, not by suppressing them. I kept the thread-per-client model and mutex design because they were clean and sufficient for the required 5+ clients.

## 5. What I Learned

I learned that TCP does not preserve message boundaries, that exact-byte reads are essential for binary file transfer, and that mutexes are mandatory as soon as two clients share users, rooms, or logging state. Testing on real hardware with `diff`, `ss`, and the log file taught me far more than any explanation could.
