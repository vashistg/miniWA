defmodule MiniWaWeb.UserSocket do
  use Phoenix.Socket
  require Logger

  # All "room:*" topics are handled by MessageChannel
  channel "room:*", MiniWaWeb.MessageChannel

  # Called on every WebSocket connection attempt.
  # We expect ?user_id=alice in the query string — no auth token yet.
  @impl true
  def connect(%{"user_id" => user_id}, socket, _connect_info) do
    Logger.info("[Socket] ── New WebSocket connection ── user_id=#{user_id}")
    {:ok, assign(socket, :user_id, user_id)}
  end

  def connect(params, _socket, _connect_info) do
    Logger.warning("[Socket] Rejected — missing user_id | params=#{inspect(params)}")
    :error
  end

  # Identifies the socket for broadcasting (used by Phoenix PubSub internally)
  @impl true
  def id(socket), do: "user_socket:#{socket.assigns.user_id}"
end
