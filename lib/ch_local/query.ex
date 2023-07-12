defmodule Ch.Local.Query do
  @moduledoc "Query struct wrapping the SQL statement."
  defstruct [:statement, :command, :encode, :decode]

  @type t :: %__MODULE__{statement: iodata, command: atom, encode: boolean, decode: boolean}

  @doc false
  @spec build(iodata, Keyword.t()) :: t
  def build(statement, opts \\ []) do
    %Ch.Query{
      statement: statement,
      command: command,
      encode: encode,
      decode: decode
    } = Ch.Query.build(statement, opts)

    %__MODULE__{
      statement: statement,
      command: command,
      encode: encode,
      decode: decode
    }
  end
end

defimpl DBConnection.Query, for: Ch.Local.Query do
  alias Ch.Local.Query
  alias Ch.{Result, RowBinary}

  @spec parse(Query.t(), Keyword.t()) :: Query.t()
  def parse(query, _opts), do: query

  @spec describe(Query.t(), Keyword.t()) :: Query.t()
  def describe(query, _opts), do: query

  @spec encode(Query.t(), params, Keyword.t()) :: {extra_flags, body}
        when params: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
             extra_flags: [{String.t(), String.t()}],
             body: iodata | Enumerable.t()

  def encode(%Query{command: :insert, encode: false, statement: statement}, data, _opts) do
    body =
      case data do
        _ when is_list(data) or is_binary(data) -> [statement, ?\n | data]
        _ -> Stream.concat([[statement, ?\n]], data)
      end

    {_extra_flags = [], body}
  end

  def encode(%Query{command: :insert, statement: statement}, params, opts) do
    cond do
      names = Keyword.get(opts, :names) ->
        types = Keyword.fetch!(opts, :types)
        header = RowBinary.encode_names_and_types(names, types)
        data = RowBinary.encode_rows(params, types)
        {_extra_flags = [], [statement, ?\n, header | data]}

      format_row_binary?(statement) ->
        types = Keyword.fetch!(opts, :types)
        data = RowBinary.encode_rows(params, types)
        {_extra_flags = [], [statement, ?\n | data]}

      true ->
        {_extra_flags = [], [query_params(params) | statement]}
    end
  end

  def encode(%Query{statement: statement}, params, opts) do
    types = Keyword.get(opts, :types)
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format
    {[{"--format", format}], [query_params(params) | statement]}
  end

  defp format_row_binary?(statement) when is_binary(statement) do
    statement |> String.trim_trailing() |> String.ends_with?("RowBinary")
  end

  defp format_row_binary?(statement) when is_list(statement) do
    statement
    |> IO.iodata_to_binary()
    |> format_row_binary?()
  end

  @spec decode(Query.t(), [binary], Keyword.t()) :: Result.t()
  def decode(%Query{command: :insert}, _data, _opts) do
    %Result{num_rows: 0, rows: nil, command: :insert, headers: []}
  end

  def decode(%Query{decode: false, command: command}, data, _opts) when is_list(data) do
    %Result{rows: data, command: command, headers: []}
  end

  def decode(%Query{command: command}, data, opts) when is_list(data) do
    case opts[:format] do
      "RowBinary" ->
        types = Keyword.fetch!(opts, :types)
        rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows(types)
        %Result{num_rows: length(rows), rows: rows, command: command, headers: []}

      "RowBinaryWithNamesAndTypes" ->
        rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows()
        %Result{num_rows: length(rows), rows: rows, command: command, headers: []}

      _other ->
        %Result{rows: data, command: command, headers: []}
    end
  end

  defp query_params(params) when is_map(params) do
    Enum.flat_map(params, fn {k, v} ->
      ["set param_", to_string(k), ?=, encode_param(v), ?;]
    end)
  end

  defp query_params(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {v, idx} ->
      ["set param_$", Integer.to_string(idx), ?=, encode_param(v), ?;]
    end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)
  defp encode_param(b) when is_binary(b), do: escape_param([{"\t", "\\t"}, {"\n", "\\n"}], b)
  defp encode_param(b) when is_boolean(b), do: Atom.to_string(b)
  defp encode_param(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  defp encode_param(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_param(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)

  defp encode_param(%DateTime{time_zone: "Etc/UTC", microsecond: microsecond} = dt) do
    seconds = DateTime.to_unix(dt, :second)

    case microsecond do
      {val, size} when size > 0 ->
        size = round(:math.pow(10, size))
        Float.to_string((seconds * size + val) / size)

      _ ->
        Integer.to_string(seconds)
    end
  end

  defp encode_param(%DateTime{} = dt) do
    raise ArgumentError, "non-UTC timezones are not supported for encoding: #{dt}"
  end

  defp encode_param(tuple) when is_tuple(tuple) do
    [?(, encode_array_params(Tuple.to_list(tuple)), ?)]
  end

  defp encode_param(a) when is_list(a) do
    [?[, encode_array_params(a), ?]]
  end

  defp encode_param(m) when is_map(m) do
    [?{, encode_map_params(Map.to_list(m)), ?}]
  end

  defp encode_array_params([last]), do: encode_array_param(last)

  defp encode_array_params([s | rest]) do
    [encode_array_param(s), ?, | encode_array_params(rest)]
  end

  defp encode_array_params([] = empty), do: empty

  defp encode_map_params([last]), do: encode_map_param(last)

  defp encode_map_params([kv | rest]) do
    [encode_map_param(kv), ?, | encode_map_params(rest)]
  end

  defp encode_map_params([] = empty), do: empty

  defp encode_array_param(s) when is_binary(s) do
    [?', escape_param([{"'", "''"}, {"\\", "\\\\"}], s), ?']
  end

  defp encode_array_param(%s{} = param) when s in [Date, NaiveDateTime] do
    [?', encode_param(param), ?']
  end

  defp encode_array_param(v), do: encode_param(v)

  defp encode_map_param({k, v}) do
    [encode_array_param(k), ?:, encode_array_param(v)]
  end

  defp escape_param([{pattern, replacement} | escapes], param) do
    param = String.replace(param, pattern, replacement)
    escape_param(escapes, param)
  end

  defp escape_param([], param), do: param
end

defimpl String.Chars, for: Ch.Local.Query do
  def to_string(%{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
