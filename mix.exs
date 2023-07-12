defmodule Ch.Local.MixProject do
  use Mix.Project

  @source_url "https://github.com/ruslandoga/ch_local"
  @version "0.1.0"

  def project do
    [
      app: :ch_local,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      name: "Ch.Local",
      description: "clickhouse-local wrapper for Elixir",
      docs: docs(),
      package: package(),
      source_url: @source_url
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ch, "~> 0.1.0"},
      {:benchee, "~> 1.0", only: [:bench]},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :docs},
      {:tz, "~> 0.26.0", only: [:test]}
    ]
  end

  defp docs do
    [
      source_url: @source_url,
      source_ref: "v#{@version}",
      main: "readme",
      # extras: ["README.md", "CHANGELOG.md"],
      extras: ["README.md"]
      # skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
