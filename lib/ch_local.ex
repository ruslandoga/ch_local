defmodule Ch.Local do
  @moduledoc "Minimal wrapper around `clickhouse-local` CLI."

  def query(statement, params \\ [], opts \\ []) do
    query = Ch.Query.build(statement, opts)

    case execute(query, params, opts) do
      {result, 0} -> {:ok, result}
      {result, code} -> {:error, code, result}
    end
  end

  defp execute(query, params, opts) do
    {cmd, cmd_args} = clickhouse_local_cmd()

    args = [
      "--path",
      opts[:path] || ".",
      "--query",
      query.statement
    ]

    param_args =
      params
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{k, v}, _idx} -> ["--param_#{k}", to_string(v)]
        {v, idx} -> ["--param_$#{idx}", to_string(v)]
      end)

    args = args ++ param_args
    args = if username = opts[:username], do: ["--username", username | args], else: args
    args = if password = opts[:password], do: ["--password", password | args], else: args
    args = if database = opts[:database], do: ["--database", database | args], else: args

    System.cmd(cmd, cmd_args ++ args, stderr_to_stdout: true)
  end

  defp clickhouse_local_cmd do
    candidates = [
      {"clickhouse-local", _args = []},
      {"clickhouse", _args = ["local"]}
    ]

    cmd_with_args = Enum.find(candidates, fn {cmd, _args} -> System.find_executable(cmd) end)

    cmd_with_args ||
      raise "could not find `clickhouse-local` nor `local` executables in path, " <>
              "please guarantee that one of them is available before running Ch.Local commands"
  end
end
