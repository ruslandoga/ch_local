defmodule Ch.LocalTest do
  use ExUnit.Case

  describe "stateless query" do
    setup do
      {:ok, conn} = Ch.Local.start_link()
      {:ok, conn: conn}
    end

    test "select", %{conn: conn} do
      assert Ch.Local.query!(conn, "select 1 + 1").rows == [[2]]
    end

    test "select with pseudo-positional params", %{conn: conn} do
      assert Ch.Local.query!(conn, "select 1 + {$0:Int16}", [1]).rows == [[2]]
    end

    test "select with named params", %{conn: conn} do
      assert Ch.Local.query!(conn, "select 1 + {a:Int16}", %{"a" => 1}).rows == [[2]]
    end
  end

  describe "stateful query" do
    setup do
      {:ok, conn} = Ch.Local.start_link(settings: [path: "./.ch"])
      {:ok, conn: conn}
    end

    test "create, select, insert, select", %{conn: conn} do
      Ch.Local.query!(conn, "drop database if exists example")
      Ch.Local.query!(conn, "create database if not exists example")

      Ch.Local.query!(conn, """
      create table if not exists example.demo(a UInt16, b String) engine MergeTree order by a
      """)

      assert Ch.Local.query!(conn, "select * from example.demo").rows == []

      types = ["UInt16", "String"]
      insert = "insert into example.demo(a, b) format RowBinary"

      Ch.Local.query!(conn, insert, [[1, "1"], [3, "3"]], types: types)
      Ch.Local.query!(conn, insert, [[2, "2"], [4, "4"]], types: types)

      assert Ch.Local.query!(conn, "select * from example.demo order by a").rows == [
               [1, "1"],
               [2, "2"],
               [3, "3"],
               [4, "4"]
             ]
    end
  end
end
