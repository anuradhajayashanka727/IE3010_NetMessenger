# Prompt Log — IE3010 NetMessenger

Registration Number: IT23695634  
Name: Anuradha Jayashanka  
Module: IE3010 — Network Programming

---

This log records the substantive AI interactions used during the development of Part 1. Each entry lists the tool used, the prompt, how the output was used, and what I changed or rejected.

---

## Entry 1 — Project setup and socket boilerplate
Tool: ChatGPT  
Prompt: "Show me the basic structure of a TCP server in C using BSD sockets — socket(), bind(), listen(), accept(), and a thread-per-client accept loop."  
Output used: Reference for the initial socket setup. Confirmed the standard order of calls and the need for `SO_REUSEADDR`.  
Modifications: I chose port **11634** (6000 + last 4 digits of my reg no.), bound to `INADDR_ANY`, and used `pthread_create` + `pthread_detach` for per-client threads instead of `fork()`.

---

## Entry 2 — Understanding TCP framing
**Tool:** ChatGPT  
**Prompt:** "Why does my server treat a single recv() call as one message? Can one recv() return partial data or multiple commands?"  
**Output used:** Explained that TCP is a byte stream with no message boundaries — one `recv()` may return half a line, multiple lines, or a header plus part of a file.  
**Modifications:** I wrote a `recv_line()` helper that accumulates bytes into a per-client buffer and only returns when a `\n` is found.

---

## Entry 3 — Multi-word message parsing
**Tool:** ChatGPT  
**Prompt:** "How should I parse commands like `BCAST <message>` where the message can contain spaces?"  
**Output used:** Suggested `sscanf("%s %s %[^\n]", cmd, arg1, arg2)`.  
**Modifications:** I **rejected** this approach after testing. It split `BCAST Hello World` incorrectly and dropped everything after the first space. I wrote my own `next_token()` function that walks a cursor through the string to extract space-separated tokens, leaving the remainder as a single string for the message body.

---

## Entry 4 — File transfer over a text protocol
**Tool:** ChatGPT  
**Prompt:** "How do I transfer a raw file over a TCP socket that also carries line-based text commands?"  
**Output used:** Explained length-prefixed framing: send `SENDFILE <target> <filename> <size>\n`, then read exactly `<size>` bytes on the receiving side. Suggested a `recv_exact()` helper.  
**Modifications:** I implemented `recv_exact()` on the server. The AI's example did not account for bytes already sitting in the receive buffer — I discovered this when files were corrupted. I then wrote `recv_exact_buffered()` (client) and adapted `recv_exact()` (server) to **drain the buffer first**, then call `recv()` on the socket for any remainder.

---

## Entry 5 — Server crash on client disconnect
**Tool:** ChatGPT  
**Prompt:** "Why does my server crash when a client disconnects unexpectedly?"  
**Output used:** Explained `SIGPIPE`: writing to a closed socket raises `SIGPIPE` which kills the process by default. Suggested `signal(SIGPIPE, SIG_IGN)` and `MSG_NOSIGNAL` on `send()`.  
**Modifications:** Applied both fixes. Also wrote `cleanup_client()` to remove the disconnected user from all rooms and broadcast `MSG PRESENCE <user> LEFT`.

---

## Entry 6 — GCC -Wformat-truncation warnings
**Tool:** ChatGPT  
**Prompt:** "GCC gives me `-Wformat-truncation` warnings on snprintf calls writing `%s` into a smaller buffer. What's the correct fix?"  
**Output used:** Explained that GCC can't prove the source string length, so it warns. Recommended using an explicit precision like `%.*s` with a max length, or `%.31s`.  
**Modifications:** Applied `%.*s` with `MAX_NAME - 1` in `create_room_locked()`, `register_client()`, and the JOIN/LEAVE response builders. I also sized local buffers to `MAX_LINE + MAX_NAME + 32` where needed. The compiler now runs completely clean with `-Wall -Wextra -Wpedantic -std=c11 -O2`.

---

## Entry 7 — Thread safety review
**Tool:** ChatGPT  
**Prompt:** "Review my server design: a shared `clients[]` array, a shared `rooms[]` array, and pthreads per client. What mutexes do I need?"  
**Output used:** Suggested a single global `state_mu` for shared state and a per-client `send_mu` for socket writes to prevent interleaved messages.  
**Modifications:** Adopted this exactly. Using one global mutex for shared state avoids the deadlock risk of acquiring two mutexes in different orders. Per-client `send_mu` is essential because broadcasts, private messages, and file transfers can all target the same socket concurrently.

---

## Entry 8 — Report and reflection structure
**Tool:** ChatGPT  
**Prompt:** "Review my Implementation Report outline against the IE3010 assignment brief (Section 2.7 and Section 4)."  
**Output used:** Suggested adding a testing summary table, an assumptions section, and explicit edge-case coverage. For the reflection, confirmed the four required prompts were covered.  
**Modifications:** I added the 20-case Testing Summary table, an Assumptions subsection in Design Rationale, and expanded the reflection with concrete examples (the corrupted file transfer, the broken `sscanf` parser).

---

## Summary of Substantive AI Use

| Category | AI involvement | My contribution |
|----------|----------------|-----------------|
| Socket boilerplate | Reference only | Personalised port, chosen thread model |
| TCP framing | Concept explained | Full implementation of `recv_line`, `recv_exact` |
| Multi-word parsing | Suggested approach | Rejected and rewrote as `next_token()` |
| File transfer | Concept explained | Full implementation including buffer draining |
| SIGPIPE handling | Explained | Applied and integrated with cleanup |
| Compiler warnings | Explained fix | Applied precision specifiers correctly |
| Thread safety | Design review | Implemented single `state_mu` + per-client `send_mu` |
| Report structure | Suggested outline | Written myself, evidence verified locally |

**Total substantive AI interactions:** 8 (all documented above).
