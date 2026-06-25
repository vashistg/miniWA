defmodule MiniWaWeb.AgentController do
  use MiniWaWeb, :controller

  def chat(conn, %{"message" => message, "history" => history, "provider" => "claude"} = params)
      when is_binary(message) and is_list(history) do
    model = Map.get(params, "model", "claude-sonnet-4-6")
    case MiniWa.Agent.AnthropicRunner.run(message, history, model) do
      {:ok, result} -> json(conn, result)
      {:error, reason} -> conn |> put_status(500) |> json(%{error: reason})
    end
  end

  def chat(conn, %{"message" => message, "history" => history})
      when is_binary(message) and is_list(history) do
    case MiniWa.Agent.Runner.run(message, history) do
      {:ok, result} -> json(conn, result)
      {:error, reason} -> conn |> put_status(500) |> json(%{error: reason})
    end
  end

  def chat(conn, _params) do
    conn |> put_status(400) |> json(%{error: "message (string) and history (array) are required"})
  end
end
