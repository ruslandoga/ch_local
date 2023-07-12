defmodule Ch.Local do
  @moduledoc "Minimal wrapper around `clickhouse-local` CLI."

  alias Ch.Local.{Connection, Query}
  alias Ch.Result

  @doc """
  Start the connection process and connect to ClickHouse.

  ## Options

  See `clickhouse local --help` for supported options. These options would be added to each query.

  ## Example

      # clickhouse-local --path . --async_insert true <...per-query params...>
      start_link(path: ".", async_insert: true)

  """
  def start_link(opts \\ []) do
    ensure_no_pool!(opts)
    DBConnection.start_link(Connection, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  def child_spec(opts) do
    ensure_no_pool!(opts)
    DBConnection.child_spec(Connection, opts)
  end

  defp ensure_no_pool!(opts) do
    if pool_size = opts[:pool_size] do
      if is_integer(pool_size) and pool_size > 1 do
        raise ArgumentError,
              "Ch.Local doesn't support pooling of more than one connection, got `[pool_size: #{pool_size}]`"
      end
    end
  end

  @doc """
  Runs a query and returns the result as `{:ok, %Ch.Result{}}` or
  `{:error, Exception.t()}` if there was a database error.

  ## Options

    * `:timeout` - Query request timeout
    * `:encode` - Whether to automatically encode `params` to `RowBinary`
    * `:decode` - Whether to automatically decode `response` from `RowBinary`
    * `:types` - ClickHouse types to use for encoding `RowBinary`
    * anything listed in `clickhouse local --help`

  ## Examples

      # echo 'set param_a=1; select 1 + {a:Int16}' | clickhouse-local --readonly true
      query(conn, "select 1 + {a:Int16}", %{"a" => 1}, readonly: true)

      # echo 'insert into example.table(column) format RowBinary\n\x01\x02\x03' | clickhouse-local --path .
      query(conn, "insert into example.table(column) format RowBinary", [[1], [2], [3]], path: ".", types: ["UInt8"])

      # echo 'select * from example.table format CSV' | clickhouse-local --path .
      query(conn, "select * from example.table format CSV", [], path: ".")

  """
  @spec query(DBConnection.conn(), iodata, params, Keyword.t()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
        when params: map | [term] | [row :: [term]] | iodata | Enumerable.t()
  def query(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, opts)

    with {:ok, _query, result} <- DBConnection.execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  @doc """
  Runs a query and returns the result or raises `Ch.Error` if
  there was an error. See `query/4`.
  """
  @spec query!(DBConnection.conn(), iodata, params, Keyword.t()) :: Result.t()
        when params: map | [term] | [row :: [term]] | iodata | Enumerable.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, opts)
    DBConnection.execute!(conn, query, params, opts)
  end
end
