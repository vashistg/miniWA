defmodule MiniWa.Session do
  use GenServer
  require Logger

  # restart: :temporary means the DynamicSupervisor won't auto-restart
  # this process on normal exit — it only dies when the user disconnects.
  def child_spec(user_id) do
    %{
      id: {__MODULE__, user_id},
      start: {__MODULE__, :start_link, [user_id]},
      restart: :temporary
    }
  end

  # ─── Client API ────────────────────────────────────────────────────────────

  def start_link(user_id) do
    GenServer.start_link(__MODULE__, user_id, name: via(user_id))
  end

  def register_channel(user_id, channel_pid) do
    GenServer.call(via(user_id), {:register_channel, channel_pid})
  end

  def send_message(from_user_id, to_user_id, content, client_id) do
    GenServer.cast(via(from_user_id), {:send_message, to_user_id, content, client_id})
  end

  def send_group_message(from_user_id, group_id, content, client_id) do
    GenServer.cast(via(from_user_id), {:send_group_message, group_id, content, client_id})
  end

  def notify_delivered(sender_user_id, message_id) do
    GenServer.cast(via(sender_user_id), {:recipient_delivered, message_id})
  end

  def notify_read(sender_user_id, message_id) do
    GenServer.cast(via(sender_user_id), {:recipient_read, message_id})
  end

  # ─── Server Callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(user_id) do
    Logger.info("""
    [Session][#{user_id}] ══════════════════════════════════════
      GenServer STARTED  pid=#{inspect(self())}
      Registered in Presence Registry (ETS) under key="#{user_id}"
    ══════════════════════════════════════════════════════════
    """)

    {:ok, %{user_id: user_id, channel_pid: nil, in_flight: %{}}}
  end

  @impl true
  def handle_call({:register_channel, channel_pid}, _from, state) do
    Logger.info("[Session][#{state.user_id}] Channel process registered | channel_pid=#{inspect(channel_pid)}")
    Process.monitor(channel_pid)
    # Trigger offline drain immediately — channel is ready to receive pushes
    send(self(), :drain_offline)
    {:reply, :ok, %{state | channel_pid: channel_pid}}
  end

  # Alice sends a message to Bob
  @impl true
  def handle_cast({:send_message, to_user_id, content, client_id}, state) do
    message_id = generate_id()

    Logger.info("""
    [Session][#{state.user_id}] ─────────── SEND ───────────────────────
      from       : #{state.user_id}
      to         : #{to_user_id}
      content    : "#{content}"
      message_id : #{message_id}
      client_id  : #{client_id}
    ────────────────────────────────────────────────────────
    """)

    message = %{
      id: message_id,
      from: state.user_id,
      to: to_user_id,
      content: content,
      client_id: client_id,
      sent_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    # ── Kafka write → tick-1 ────────────────────────────────────────────────
    # Publish to Kafka synchronously. Broker ack = Kafka has it durably.
    # Tick-1 goes to Alice the moment Kafka confirms — ScyllaDB write and
    # delivery to Bob happen asynchronously in the Consumer.
    Logger.info("[Session][#{state.user_id}] Publishing to Kafka | message_id=#{message_id}")

    case MiniWa.Streaming.Producer.publish(message) do
      :ok ->
        Logger.info("[Session][#{state.user_id}] ✓ Kafka confirmed — sending tick-1 to Alice")
        push_to_channel(state.channel_pid, {:tick1, message})
        {:noreply, %{state | in_flight: Map.put(state.in_flight, message_id, message)}}

      {:error, reason} ->
        Logger.error("""
        [Session][#{state.user_id}] ✗ Kafka publish FAILED — NOT sending tick-1
          reason     : #{inspect(reason)}
          message_id : #{message_id}
          Is Kafka running? Check docker-compose logs.
        """)
        {:noreply, state}
    end
  end

  # Group message — publish to Kafka, Consumer handles member fan-out
  @impl true
  def handle_cast({:send_group_message, group_id, content, client_id}, state) do
    message_id = generate_id()

    Logger.info("""
    [Session][#{state.user_id}] ─── GROUP SEND ───────────────────────
      from       : #{state.user_id}
      group      : #{group_id}
      content    : "#{content}"
      message_id : #{message_id}
    """)

    message = %{
      id:              message_id,
      type:            "group",
      from:            state.user_id,
      to:              group_id,
      content:         content,
      client_id:       client_id,
      sent_at:         DateTime.utc_now() |> DateTime.to_iso8601(),
      conversation_id: group_id
    }

    case MiniWa.Streaming.Producer.publish(message) do
      :ok ->
        Logger.info("[Session][#{state.user_id}] ✓ Group message in Kafka — sending tick-1")
        push_to_channel(state.channel_pid, {:tick1, message})
        {:noreply, %{state | in_flight: Map.put(state.in_flight, message_id, message)}}

      {:error, reason} ->
        Logger.error("[Session][#{state.user_id}] ✗ Kafka publish failed: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  # Incoming message from another user's Session via process message
  @impl true
  def handle_cast({:deliver, message}, state) do
    Logger.info("""
    [Session][#{state.user_id}] ─────────── RECEIVE ─────────────────────
      from    : #{message.from}
      id      : #{message.id}
      content : "#{message.content}"
      path    : #{message.from}.Session ──[process msg]──▶ #{state.user_id}.Session ──▶ WebSocket
    ────────────────────────────────────────────────────────
    """)

    push_to_channel(state.channel_pid, {:incoming_message, message})
    {:noreply, state}
  end

  # Recipient confirmed delivery → send tick-2 back to original sender
  @impl true
  def handle_cast({:recipient_delivered, message_id}, state) do
    Logger.info("[Session][#{state.user_id}] ✓✓ DELIVERED ACK received | message_id=#{message_id} → pushing tick-2 to Alice's client")
    push_to_channel(state.channel_pid, {:tick2, message_id})
    {:noreply, %{state | in_flight: Map.delete(state.in_flight, message_id)}}
  end

  # Recipient read the message → send tick-3 back to original sender
  @impl true
  def handle_cast({:recipient_read, message_id}, state) do
    Logger.info("[Session][#{state.user_id}] ✓✓✓ READ ACK received | message_id=#{message_id} → pushing tick-3 to Alice's client")
    push_to_channel(state.channel_pid, {:tick3, message_id})
    {:noreply, state}
  end

  # On every connect/reconnect: drain any messages that arrived while offline
  @impl true
  def handle_info(:drain_offline, state) do
    Logger.info("[Session][#{state.user_id}] Checking ScyllaDB for undelivered messages...")

    case MiniWa.DB.fetch_undelivered(state.user_id) do
      {:ok, []} ->
        Logger.info("[Session][#{state.user_id}] No undelivered messages — inbox clean")

      {:ok, messages} ->
        Logger.info("""
        [Session][#{state.user_id}] ─────────── OFFLINE DRAIN ─────────────────
          Found #{length(messages)} undelivered message(s) in ScyllaDB
          Pushing to WebSocket now...
        """)

        Enum.each(messages, fn message ->
          Logger.info("[Session][#{state.user_id}] Draining message_id=#{message.id} from=#{message.from}")
          push_to_channel(state.channel_pid, {:incoming_message, message})
        end)

      {:error, reason} ->
        Logger.error("[Session][#{state.user_id}] Drain failed: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  # Forward group invite notification to the client
  @impl true
  def handle_info({:group_invite, group}, state) do
    push_to_channel(state.channel_pid, {:group_invite, group})
    {:noreply, state}
  end

  # Forward group removal notification to the client
  @impl true
  def handle_info({:removed_from_group, payload}, state) do
    push_to_channel(state.channel_pid, {:removed_from_group, payload})
    {:noreply, state}
  end

  # Channel (WebSocket) process died → user disconnected → stop this Session
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.info("""
    [Session][#{state.user_id}] Channel process died
      pid    : #{inspect(pid)}
      reason : #{inspect(reason)}
    → Stopping Session. '#{state.user_id}' is now OFFLINE (removed from Registry).
    """)

    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("[Session][#{state.user_id}] Session terminated | reason=#{inspect(reason)}")
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp via(user_id), do: {:via, Registry, {MiniWa.Presence.Registry, user_id}}

  defp push_to_channel(nil, event) do
    Logger.warning("[Session] No channel registered, dropping event: #{inspect(event)}")
  end

  defp push_to_channel(channel_pid, event), do: send(channel_pid, event)

  defp generate_id do
    # Time-ordered ID: 12-char hex timestamp (ms) + 8-char random suffix.
    # Lexicographic sort = chronological sort, which means undelivered_messages
    # drains in send order without any extra sorting.
    ts  = System.system_time(:millisecond) |> Integer.to_string(16) |> String.pad_leading(12, "0")
    rnd = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    ts <> rnd
  end
end
