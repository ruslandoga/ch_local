defmodule Ch.Local.Connection do
  @moduledoc false

  use DBConnection
  require Logger

  alias Ch.Local.Query
  alias Ch.{Error, Result}

  @impl true
  @spec connect(Keyword.t()) :: {:ok, keyword} | {:error, Error.t()}
  def connect(opts) do
    handshake = Query.build("select 1")
    params = DBConnection.Query.encode(handshake, _params = [], _opts = [])

    common_flags =
      Keyword.drop(opts, [:timeout, :pool_index])
      |> Enum.map(fn {k, v} -> {"--#{k}", to_string(v)} end)

    case handle_execute(handshake, params, _opts = [], common_flags) do
      {:ok, handshake, responses, ^common_flags} ->
        case DBConnection.Query.decode(handshake, responses, _opts = []) do
          %Result{rows: [[1]]} ->
            {:ok, common_flags}

          result ->
            {:error, Error.exception("unexpected result for '#{handshake}': " <> inspect(result))}
        end

      {:error, reason, _common_flags} ->
        {:error, reason}
    end
  end

  @impl true
  def ping(state), do: {:ok, state}

  @impl true
  def checkout(state), do: {:ok, state}

  # we "support" these four tx callbacks for Repo.checkout
  # even though ClickHouse doesn't support txs

  @impl true
  def handle_begin(_opts, state), do: {:ok, %{}, state}
  @impl true
  def handle_commit(_opts, state), do: {:ok, %{}, state}
  @impl true
  def handle_rollback(_opts, state), do: {:ok, %{}, state}
  @impl true
  def handle_status(_opts, state), do: {:idle, state}

  @impl true
  def handle_prepare(_query, _opts, state) do
    {:error, Error.exception("prepared statements are not supported"), state}
  end

  @impl true
  def handle_close(_query, _opts, state) do
    {:error, Error.exception("prepared statements are not supported"), state}
  end

  @impl true
  def handle_declare(_query, _params, _opts, state) do
    {:error, Error.exception("cursors are not yet supported"), state}
  end

  @impl true
  def handle_fetch(_query, _params, _opts, state) do
    {:error, Error.exception("cursors are not yet supported"), state}
  end

  @impl true
  def handle_deallocate(_query, _ref, _opts, state) do
    {:error, Error.exception("cursors are not yet supported"), state}
  end

  @impl true
  def handle_execute(query, params, _opts, common_flags) do
    {extra_flags, body} = params
    flags = common_flags ++ extra_flags

    case exec(flags, body) do
      {:ok, responses} -> {:ok, query, responses, common_flags}
      {:error, reason} -> {:error, reason, common_flags}
    end
  end

  @impl true
  def disconnect(_error, _state), do: :ok

  defp clickhouse_local_cmd do
    candidates = [
      {"clickhouse-local", _args = []},
      {"clickhouse", _args = ["local"]}
    ]

    cmd_with_args =
      Enum.find_value(candidates, fn {cmd, args} ->
        if cmd = System.find_executable(cmd), do: {cmd, args}
      end)

    cmd_with_args ||
      raise "could not find `clickhouse-local` nor `local` executables in path, " <>
              "please guarantee that one of them is available before running Ch.Local commands"
  end

  # TODO timeout?
  @doc false
  def exec(flags, body, receive? \\ false, timeout \\ :infinity) do
    {cmd, args} = clickhouse_local_cmd()

    port =
      Port.open(
        {:spawn_executable, cmd},
        [
          :use_stdio,
          :exit_status,
          :binary,
          :hide,
          args: args ++ Enum.flat_map(flags, fn {k, v} -> [k, v] end)
        ]
      )

    try do
      if is_function(body, 2) do
        Enum.each(body, fn chunk ->
          true = Port.command(port, chunk)
        end)
      else
        true = Port.command(port, body)
      end

      if receive?, do: recv_exec(port, timeout, _acc = [])
    after
      Port.close(port)
    end
  end

  defp recv_exec(port, timeout, acc) do
    receive do
      {^port, {:data, data}} ->
        recv_exec(port, timeout, [data | acc])

      {^port, {:exit_status, status}} ->
        acc = :lists.reverse(acc)

        case status do
          0 -> {:ok, acc}
          _ -> {:error, Ch.Error.exception(code: status, message: IO.iodata_to_binary(acc))}
        end
    after
      timeout ->
        # TODO Ch.Local.Error{reason: :timeout}
        {:error, :timeout}
    end
  end
end
