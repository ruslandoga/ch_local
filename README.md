# Ch.Local

`clickhouse-local` wrapper for Elixir.

## Installation

```elixir
defp deps do
  [
    {:ch_local, "~> 0.1.0"}
  ]
end
```

## Usage

#### Download and install `clickhouse` or `clickhouse-local` executables

```console
$ curl https://clickhouse.com/ | sh
$ ./clickhouse install
$ clickhouse-local --version
```

#### Start the connection

```elixir
defaults = [
  settings: [],
  timeout: :timer.seconds(15)
]

{:ok, pid} = Ch.Local.start_link(defaults)
```

#### Stateless

```elixir
{:ok, pid} = Ch.Local.start_link()

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.Local.query(pid, "SELECT * FROM system.numbers LIMIT 3")

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.Local.query(pid, "SELECT * FROM system.numbers LIMIT {$0:UInt8}", [3])

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.Local.query(pid, "SELECT * FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 3})
```

#### Stateful

```elixir
{:ok, pid} = Ch.Local.start_link(settings: [path: "./demo"])

# creates ./demo/data/demo dir
Ch.Local.query!(pid, "CREATE DATABASE demo")
# creates ./demo/data/demo/example dir
Ch.Local.query!(pid, "CREATE TABLE demo.example(a UInt64, b String) ENGINE MergeTree ORDER BY a")
# creates ./demo/data/demo/example/all_1_1_0 block
Ch.Local.query!(pid, "INSERT INTO demo.example(a, b) FORMAT RowBinary", [[1, "2"], [3, "4"]], types: ["UInt64", "String"])
# creates ./demo/data/demo/example/all_2_2_0 block
Ch.Local.query!(pid, "INSERT INTO demo.example(b, a) FORMAT RowBinary", [["8", 7], ["6", 5]], types: [:string, :u64])
# merges these two blocks into ./demo/data/demo/example/all_1_2_1
# btw the naming is all_<min>_<max>_<level>
Ch.Local.query!(pid, "OPTIMIZE TABLE demo.example")

# note that OPTIMIZE TABLE is not removing stale blocks -- that's up to you
{:ok, ["detached", "format_version.txt", "all_1_1_0", "all_1_2_1", "all_2_2_0"]} =
  File.ls("./demo/data/demo/example")

{:ok, %Ch.Result{rows: [[1, "2"], [3, "4"], [5, "6"], [7, "8"]]}} =
  Ch.Local.query(pid, "SELECT * FROM demo.example ORDER BY a")
```

Please see [Ch](https://github.com/plausible/ch) for more usage details.
