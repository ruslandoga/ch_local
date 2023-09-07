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

Ch.Local.query!(pid, "CREATE DATABASE demo")
Ch.Local.query!(pid, "CREATE TABLE demo.example(a UInt64, b String) ENGINE MergeTree ORDER BY a")
Ch.Local.query!(pid, "INSERT INTO demo.example(a, b) FORMAT RowBinary", [[1, "2"], [3, "4"]], types: ["UInt64", "String"])

{:ok, %Ch.Result{rows: [[1, "2"], [3, "4"]]}} =
  Ch.Local.query(pid, "select * from demo.example order by a")
```

Please see [Ch](https://github.com/plausible/ch) for more usage details.
