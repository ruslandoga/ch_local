defmodule Ch.LocalTest do
  use ExUnit.Case

  describe "query" do
    test "select" do
      assert {:ok, %Ch.Result{rows: [[2]]}} = Ch.Local.query("select 1 + 1")
    end

    test "select with pseudo-positional params" do
      assert {:ok, %Ch.Result{rows: [[2]]}} = Ch.Local.query("select 1 + {$0:Int16}", [1])
    end

    test "select with named params" do
      assert {:ok, %Ch.Result{rows: [[2]]}} = Ch.Local.query("select 1 + {a:Int16}", %{"a" => 1})
    end
  end
end
