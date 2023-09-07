defmodule Ch.Local.Connection do
  @moduledoc false

  use DBConnection
  require Logger

  alias Ch.Local.Query
  alias Ch.{Error, Result}

  @impl true
  @spec connect(Keyword.t()) :: {:ok, map} | {:error, Error.t()}
  def connect(opts) do
    handshake = Query.build("select 1")
    params = DBConnection.Query.encode(handshake, _params = [], _opts = [])

    config =
      %{
        timeout: opts[:timeout] || :timer.seconds(15),
        settings: opts[:settings] || [],
        cmd: Keyword.fetch!(opts, :cmd)
      }

    case handle_execute(handshake, params, _opts = [], config) do
      {:ok, handshake, responses, _config} ->
        case DBConnection.Query.decode(handshake, responses, _opts = []) do
          %Result{rows: [[1]]} ->
            {:ok, config}

          result ->
            {:error, Error.exception("unexpected result for '#{handshake}': " <> inspect(result))}
        end

      {:error, reason, _config} ->
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
  def handle_execute(query, body, opts, config) do
    %{cmd: cmd, settings: settings, timeout: timeout} = config
    timeout = opts[:timeout] || timeout
    extra_settings = opts[:settings] || []

    default_format =
      if Keyword.get(opts, :types) do
        "RowBinary"
      else
        "RowBinaryWithNamesAndTypes"
      end

    format = Keyword.get(extra_settings, :format) || default_format
    settings = settings |> Keyword.merge(extra_settings) |> Keyword.put(:format, format)

    case exec(cmd, settings, body, timeout) do
      {:ok, responses} -> {:ok, query, responses, config}
      {:error, reason} -> {:error, reason, config}
    end
  end

  @impl true
  def disconnect(_error, _state), do: :ok

  require Logger

  @doc false
  def exec(cmd, settings, body, timeout) do
    {cmd, args} =
      case cmd do
        {_cmd, _args} -> cmd
        cmd when is_binary(cmd) -> {cmd, _no_args = []}
      end

    flags = Enum.flat_map(settings, fn {k, v} -> ["--" <> to_string(k), to_string(v)] end)

    task =
      Task.async(fn ->
        case Rambo.run(cmd, args ++ flags, in: body, timeout: timeout, log: &IO.inspect/1) do
          {:ok, %Rambo{out: out, status: 0}} ->
            {:ok, out}

          {:error, %Rambo{out: out, status: status}} ->
            {:error, Ch.Error.exception(code: status, message: out)}

          {:killed, %Rambo{}} ->
            {:error, Ch.Error.exception(message: "killed")}
        end
      end)

    Task.await(task, timeout)
  end
end
