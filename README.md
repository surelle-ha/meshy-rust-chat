# Meshy Rust Chat

A lightweight, ad-hoc, peer-to-peer terminal chat application built in **Rust**.  
Meshy Rust Chat allows devices on the same network to discover each other and exchange messages without a central server.

---

## Introduction

Meshy Rust Chat is designed for simplicity, speed, and portability.  
It leverages Rust’s asynchronous ecosystem (`tokio`) for TCP/UDP networking and provides real-time terminal messaging between peers.  

Key features:
- Peer discovery via UDP broadcast
- TCP-based peer-to-peer messaging
- Terminal-friendly interface
- Lightweight and Rust-native

---

## Standard Operating Procedure (SOP)

1. **Starting a Node**  
   - Each user starts their own instance of Meshy Rust Chat in the terminal.
   - The app broadcasts its presence over the local network via UDP.
   - Other peers automatically detect and connect to it over TCP.

2. **Messaging**
   - Type messages and press **Enter** to send to all connected peers.
   - Incoming messages appear in the terminal with the peer’s name.

3. **Shutting Down**
   - Press **Ctrl+C** to safely disconnect and close all connections.
   - All peer connections are gracefully shut down.

---

## Usage

```bash
# Build the project
cargo build --release

# Run the chat client
# ./target/release/meshy-rust-chat <name> <tcp_port> <udp_discovery_port>

# Example:
./target/debug/meshy-rust-chat harold 39319 9001
./target/debug/meshy-rust-chat alice 39320 9001
```

- `<name>` – your username in the chat  
- `<tcp_port>` – optional TCP port (default: random available)  
- `<udp_discovery_port>` – optional UDP discovery port (default: 9001)  

**Notes:**
- Ensure all devices are on the same local network for discovery to work.
- Multiple instances can run on the same device with different TCP ports.

---

## Development Instructions

1. **Clone the repository**
```bash
git clone https://github.com/surelle-ha/meshy-rust-chat.git
cd meshy-rust-chat
```

2. **Install Rust toolchain** (if not installed)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update
```

3. **Run the project**
```bash
cargo run -- <name> [tcp_port] [udp_discovery_port]
```

4. **Run tests**
```bash
cargo test
```

5. **Build release**
```bash
cargo build --release
```

---

## Collaboration Instructions

We welcome contributors! Here’s how to collaborate:

1. **Fork the repository** and create a feature branch:
```bash
git checkout -b feature/awesome-feature
```

2. **Make your changes** in Rust following the existing project style.

3. **Run tests** to ensure nothing breaks:
```bash
cargo test
```

4. **Commit and push**
```bash
git add .
git commit -m "Add awesome feature"
git push origin feature/awesome-feature
```

5. **Open a Pull Request** describing your changes.  
   - Include screenshots or logs if applicable.
   - Mention any potential breaking changes or TODOs.

6. **Code Review & Merge**
   - Once approved, PR will be merged into the `main` branch.

---

## License

MIT License © 2025 Meshy Rust Chat Team

---

### Acknowledgements

- Rust Programming Language – [https://www.rust-lang.org](https://www.rust-lang.org)  
- Tokio Async Runtime – [https://tokio.rs](https://tokio.rs)  
- Whoami crate for username detection – [https://crates.io/crates/whoami](https://crates.io/crates/whoami)  

---

> **Tip:** Use unique usernames per device to avoid self-message echoing.
