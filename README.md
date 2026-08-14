<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Logo" />
  <h1>SnerdMQ v0.2.0</h1>
  <p>The AI-Era polyglot background job queue daemon powered by Rust.</p>

  [![Crates.io](https://img.shields.io/crates/v/snerdmq)](https://crates.io/crates/snerdmq)
  [![License](https://img.shields.io/crates/l/snerdmq)](https://github.com/greyhands2/snerdmq/blob/main/LICENSE)
</div>

`snerdmq` is a specialized, embedded sidecar daemon that handles complex queue orchestration (file locking, retries, dead-letter queues, and now **AI orchestration**) in highly-optimized Rust. It lets you write your execution logic natively in **Node.js, Python, Go, Ruby, PHP, Java, or C#**.

It runs as a child process and communicates via incredibly fast JSON over standard I/O pipes.

## ✨ v0.2.0 "AI-Era" Features

Traditional message brokers force you to manage external servers. **SnerdMQ eliminates the network entirely** while bringing advanced orchestration specifically designed for AI workloads:

- **Smart API Rate-Limiting**: Natively tracks `rate_limit_group` execution velocity. If you burst hundreds of LLM generation jobs, SnerdMQ pauses dispatching to prevent 429 "Too Many Requests" HTTP errors.
- **Payload-Hashing Deduplication**: Automatically computes a cryptographic hash of your `task_data`. If an identical payload is in the queue (`auto_dedupe`), it silently drops the duplicate.
- **Dynamic Float Prioritization**: A true Binary Max-Heap sorts pending jobs by an `urgency_score` (e.g., `0.95`). High-priority AI tasks bypass the standard FIFO queue with 0ms latency.
- **Progress Streaming & Dashboard**: SDKs can emit `yieldProgress` partial chunks (ideal for streaming LLM tokens). The SnerdMQ daemon multiplexes these updates to a **built-in React UI dashboard** running on an embedded HTTP/WebSocket server.
- **Bulletproof Durability**: `fs3` OS-level file locking ensures 100% ACID compliance and corruption-free local storage.


### The JSON Protocol
SnerdMQ expects simple JSON objects over STDIN. Here is exactly what an advanced AI task payload looks like:

```json
{
  "action": "enqueue",
  "task_type": "generate_llm_response",
  "task_id": "req_9921",
  "task_data": { "prompt": "Explain quantum physics to a toddler" },
  
  // v0.2.0 AI-Era Features
  "auto_dedupe": true,              // Silently drop if this payload is already in the queue
  "urgency_score": 0.95,            // Bypass standard FIFO queue; float to the top
  "rate_limit_group": "anthropic",  // Group for backpressure
  "max_per_minute": 50              // Prevent 429 API errors
}
```

*Note: You rarely have to write this JSON yourself! The official Thin Client SDKs handle all of this automatically.*


### ⚙️ Advanced Task Configuration (v0.2.0)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`auto_dedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the `task_type` and `task_data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgency_score` (`float`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rate_limit_group` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`max_per_minute` (`int`)**: Used in conjunction with `rate_limit_group`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.

## ⚡ Architecture (Zero Networking)

<div align="center">
  <img src="./assets/architecture.gif" alt="SnerdMQ v0.2.0 Architecture" />
  <br/>
  <i>Zero-latency embedded queue orchestration featuring Real-Time Tracking</i>
</div>

## 📦 Installation

**Option 1: Cargo (For developers with Rust installed)**
```bash
cargo install snerdmq
```

**Option 2: Pre-compiled Binaries (For production servers)**
Download the appropriate binary for your OS from the [GitHub Releases](https://github.com/greyhands2/snerdmq/releases) page.

## 🌍 Distributed Scaling (Kubernetes / EC2)

By default, `snerdmq` stores its queue in a local file at `.snerdata/tasks/tasks.log`. 
To scale horizontally across multiple isolated servers, simply mount a **Shared Network Drive (like AWS EFS or an NFS volume)** to all of your servers:

```bash
# All 10 servers point to the exact same shared file!
./snerdmq /mnt/aws-efs-shared-drive/snerd_tasks.log
```
Native OS file locking (`flock`) guarantees zero data corruption across your cluster—no Redis required.

## 🐳 Docker & Cloud Deployments (ECS / K8s / Droplets)

Because `snerdmq` runs directly over standard I/O pipes rather than TCP, you do **not** deploy it as a standalone microservice container! Instead, you package the ultra-lightweight daemon binary directly inside your main application's Docker image:

```dockerfile
# Your standard application image
FROM node:20-alpine

# Simply copy the SnerdMQ binary into your application container
COPY --from=greyhands2/snerdmq-release /bin/snerdmq /usr/local/bin/snerdmq

# Your application's SDK will automatically spawn the daemon internally!
CMD ["node", "app.js"]
```

**Single-Node (DigitalOcean Droplets / EC2):** 
If you are running a single container on a VPS, SnerdMQ will simply write to the container's local SSD. You get insane sub-millisecond performance with zero configuration.

**Multi-Node Auto-Scaling (AWS ECS / Kubernetes / Fargate):**
When scaling horizontally across a cluster, just mount an **Amazon EFS** (or any NFS/Shared Volume) to your containers and point SnerdMQ to it. The native POSIX file-locking handles all cross-container cluster synchronization perfectly—no Redis cluster required!

---

## 🏗 Ecosystem: Embedded vs Sidecar

Depending on your language, the SnerdMQ ecosystem offers two distinct ways to run background jobs.

### For Rust Developers
- Use [**`snerd-rust`**](https://github.com/greyhands2/snerd-rust): This is the **Embedded Library**. Best for pure Rust microservices that want to compile the queue directly into their binary for a zero-dependency, single-file deployment.
- Use [**`snerdmq`**](https://github.com/greyhands2/snerdmq): This is the **Sidecar Daemon**. Best for polyglot systems, or when developers want strict process isolation (so an application crash/panic doesn't kill the queue orchestrator).

**Example: Calling the SnerdMQ Sidecar Daemon from Rust**
If you are using the Sidecar daemon in Rust instead of the embedded library, you can spawn the daemon as a child process and pass JSON payloads instantly over standard I/O:

```rust
use std::process::{Command, Stdio};
use std::io::Write;
use serde_json::json;

fn main() {
    // Spawn the daemon
    let mut child = Command::new("snerdmq")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn SnerdMQ daemon");

    // Write a task over STDIN using the JSON protocol
    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    
    let task = json!({
        "action": "enqueue",
        "task_type": "rust_heavy_compute",
        "task_id": "rust_001",
        "task_data": { "matrix_size": 1000 },
        "auto_dedupe": true,
        "urgency_score": 0.99
    });

    writeln!(stdin, "{}", task.to_string()).unwrap();
}
```


### For Go Developers
- Use [**`snerd-go`**](https://github.com/greyhands2/snerd-go): This is the **Embedded Library**. Best for pure Go applications that want native Goroutine orchestration without needing to bundle or download a pre-compiled Rust binary.
- Use [**`snerdmq-go`**](https://github.com/greyhands2/snerdmq-go): This is the **Thin Client SDK**. Best for Go apps running in a polyglot microservices cluster where all microservices (Node, Python, Go) need to share the exact same queue storage format and Rust-powered `fs3` file-locking engine.

---

## 🔧 Official SDKs (Thin Clients)
To communicate with the `snerdmq` daemon effortlessly, use our official Thin Client SDKs:
- [x] [Node.js / TypeScript (snerdmq-node)](https://www.npmjs.com/package/snerdmq-node)
- [x] [Python (snerdmq-python)](https://pypi.org/project/snerdmq-python/)
- [x] [Go (snerdmq-go)](https://pkg.go.dev/github.com/greyhands2/snerdmq-go)
- [x] [Ruby (snerdmq-ruby)](https://rubygems.org/gems/snerdmq)
- [x] [PHP (snerdmq-php)](https://packagist.org/packages/greyhands2/snerdmq)
- [x] [Java / Kotlin (snerdmq-java)](https://central.sonatype.com/artifact/io.github.greyhands2/snerdmq)
- [x] [C# / .NET (snerdmq-dotnet)](https://www.nuget.org/packages/SnerdMQ)

*Built with ❤️ for John Wick tier engineering.*
