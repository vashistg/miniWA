defmodule MiniWaWeb.MessageChannel do
  use MiniWaWeb, :channel
  require Logger

  alias MiniWa.Session

  # ─── Join ─────────────────────────────────────────────────────────────────

  # Client joins "room:<their_own_user_id>".
  # We only allow joining your own room — no eavesdropping.
  @impl true
  def join("room:" <> user_id, _params, socket) do
    if socket.assigns.user_id == user_id do
      Logger.info("""
      [Channel][#{user_id}] ══════════════════════════════════════
        Client JOINED channel room:#{user_id}
        channel_pid : #{inspect(self())}
      ══════════════════════════════════════════════════════════
      """)

      # Start a Session GenServer for this user under the DynamicSupervisor.
      # restart: :temporary means it won't be restarted when it stops normally.
      case DynamicSupervisor.start_child(MiniWa.Session.Supervisor, {Session, user_id}) do
        {:ok, pid} ->
          Logger.info("[Channel][#{user_id}] DynamicSupervisor spawned new Session | pid=#{inspect(pid)}")

        {:error, {:already_started, pid}} ->
          Logger.info("[Channel][#{user_id}] Session already running (reconnect) | pid=#{inspect(pid)}")

        error ->
          Logger.error("[Channel][#{user_id}] Failed to start Session: #{inspect(error)}")
      end

      # Tell the Session which process to push WebSocket events to (this channel)
      :ok = Session.register_channel(user_id, self())

      # Persist user to ScyllaDB (idempotent — safe on every connect)
      MiniWa.DB.register_user(user_id)

      # Subscribe this channel process to presence broadcasts from all users
      Phoenix.PubSub.subscribe(MiniWa.PubSub, "presence")

      # Subscribe to typing events addressed to this user (1:1 path)
      Phoenix.PubSub.subscribe(MiniWa.PubSub, "typing:user:#{user_id}")

      # Broadcast to everyone already connected that this user is now online
      Phoenix.PubSub.broadcast(MiniWa.PubSub, "presence", {:presence_join, user_id})
      Logger.info("[Channel][#{user_id}] Broadcasted presence_join")

      users  = all_users_with_status()
      groups = case MiniWa.DB.list_user_groups(user_id) do
        {:ok, g} -> g
        {:error, _} -> []
      end

      # Subscribe to typing events for every group this user belongs to
      Enum.each(groups, fn %{group_id: gid} ->
        Phoenix.PubSub.subscribe(MiniWa.PubSub, "typing:group:#{gid}")
      end)

      Logger.info("[Channel][#{user_id}] users=#{length(users)} groups=#{length(groups)}")
      {:ok, %{users: users, groups: groups}, socket}
    else
      Logger.warning("""
      [Channel] Unauthorized join
        socket_user : #{socket.assigns.user_id}
        tried room  : room:#{user_id}
      """)

      {:error, %{reason: "unauthorized"}}
    end
  end

  # ─── Client → Server events ───────────────────────────────────────────────

  # Alice sends a message to Bob
  @impl true
  def handle_in("send_msg", params, socket) do
    %{"to" => to, "client_id" => client_id} = params
    content        = Map.get(params, "content", "")
    client_sent_at = Map.get(params, "client_sent_at")
    media_url      = Map.get(params, "media_url")
    media_type     = Map.get(params, "media_type")
    user_id = socket.assigns.user_id
    Logger.info("[Channel][#{user_id}] send_msg ─▶ Session | to=#{to} client_id=#{client_id}")
    Session.send_message(user_id, to, content, client_id, client_sent_at, media_url, media_type)
    {:noreply, socket}
  end

  # Bob's client confirms it received the message (tick-2 path).
  # Two things happen here:
  #   1. Remove from undelivered_messages — Bob has the message, no need to drain it again
  #   2. Notify the sender's Session so it can push tick-2 to Alice
  def handle_in("delivered", %{"message_id" => message_id, "from" => from}, socket) do
    user_id = socket.assigns.user_id
    Logger.info("[Channel][#{user_id}] delivered ACK | message_id=#{message_id} from=#{from}")

    # Step 1: clean up offline queue (idempotent — safe even if message was never queued)
    MiniWa.DB.mark_delivered(user_id, message_id)

    # Step 2: propagate tick-2 to sender
    Session.notify_delivered(from, message_id)

    {:noreply, socket}
  end

  # Create a new group and add initial members
  def handle_in("create_group", %{"name" => name, "members" => members}, socket) do
    creator  = socket.assigns.user_id
    group_id = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    Logger.info("[Channel][#{creator}] create_group | name=#{name} members=#{inspect(members)}")

    case MiniWa.DB.create_group(group_id, name, creator) do
      :ok ->
        Enum.each(members, fn uid ->
          if uid != creator do
            MiniWa.DB.add_group_member(group_id, name, uid, creator, 0)
            notify_group_invite(uid, group_id, name, creator)
          end
        end)
        {:reply, {:ok, %{group_id: group_id, name: name}}, socket}

      {:error, reason} ->
        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  # Send a message to a group
  def handle_in("send_group_msg", params, socket) do
    %{"group_id" => group_id, "client_id" => client_id} = params
    content        = Map.get(params, "content", "")
    client_sent_at = Map.get(params, "client_sent_at")
    media_url      = Map.get(params, "media_url")
    media_type     = Map.get(params, "media_type")
    user_id = socket.assigns.user_id
    Logger.info("[Channel][#{user_id}] send_group_msg → group=#{group_id}")
    Session.send_group_message(user_id, group_id, content, client_id, client_sent_at, media_url, media_type)
    {:noreply, socket}
  end

  # Typing indicator — ephemeral, no Kafka/ScyllaDB, pure PubSub.
  # conv_id must match what the RECIPIENT has as activeConv:
  #   1:1   → recipient's activeConv = sender's user_id  → conv_id = from
  #   group → everyone's activeConv  = group_id          → conv_id = group_id
  def handle_in("typing", %{"to" => to}, socket) do
    from = socket.assigns.user_id
    Phoenix.PubSub.broadcast(MiniWa.PubSub, "typing:user:#{to}", {:typing, from, from})
    {:noreply, socket}
  end

  def handle_in("typing", %{"group_id" => group_id}, socket) do
    from = socket.assigns.user_id
    Phoenix.PubSub.broadcast(MiniWa.PubSub, "typing:group:#{group_id}", {:typing, from, group_id})
    {:noreply, socket}
  end

  # Return current member list for a group (used by manage-members modal)
  def handle_in("get_group_members", %{"group_id" => group_id}, socket) do
    case MiniWa.DB.list_group_members(group_id) do
      {:ok, members} -> {:reply, {:ok, %{members: members}}, socket}
      {:error, reason} -> {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  # Remove a member from a group
  def handle_in("remove_from_group", %{"group_id" => group_id, "user_id" => uid}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] remove_from_group | group=#{group_id} user=#{uid}")
    case MiniWa.DB.remove_group_member(group_id, uid) do
      :ok ->
        # Notify the removed user if they're online so their sidebar updates
        case Registry.lookup(MiniWa.Presence.Registry, uid) do
          [{pid, _}] -> send(pid, {:removed_from_group, %{group_id: group_id}})
          [] -> :offline
        end
        {:reply, :ok, socket}
      {:error, reason} ->
        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  # Add a member to an existing group, optionally sharing history from a timestamp
  def handle_in("add_to_group", %{"group_id" => group_id, "user_id" => uid, "share_from" => share_from_ms}, socket) do
    added_by = socket.assigns.user_id
    Logger.info("[Channel][#{added_by}] add_to_group | group=#{group_id} user=#{uid} share_from=#{share_from_ms}")

    # Fetch group name for the new member's user_groups entry
    group_name = case MiniWa.DB.list_user_groups(added_by) do
      {:ok, groups} ->
        case Enum.find(groups, fn g -> g.group_id == group_id end) do
          nil   -> group_id
          found -> found.name
        end
      _ -> group_id
    end

    case MiniWa.DB.add_group_member(group_id, group_name, uid, added_by, share_from_ms) do
      :ok ->
        # Always attempt history push — fetch_group_history returns [] if nothing qualifies.
        # share_from_ms = 0 means "from the beginning"; Date.now() means "from now" (no history).
        push_history_to_member(uid, group_id, share_from_ms)
        notify_group_invite(uid, group_id, group_name, added_by)
        {:reply, :ok, socket}

      {:error, reason} ->
        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  # Bob's client confirms it was read (tick-3 path)
  def handle_in("read", %{"message_id" => message_id, "from" => from}, socket) do
    user_id = socket.assigns.user_id
    Logger.info("[Channel][#{user_id}] read ACK ─▶ notifying sender=#{from} | message_id=#{message_id}")
    Session.notify_read(from, message_id)
    {:noreply, socket}
  end

  # ─── Session → Channel pushes (server → client via WebSocket) ─────────────

  # Tick-1: server has received and persisted the message
  @impl true
  def handle_info({:tick1, message}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] Session ─▶ WS tick1 | client_id=#{message.client_id} message_id=#{message.id}")
    push(socket, "tick1", %{
      client_id:             message.client_id,
      message_id:            message.id,
      kafka_published_at_ms: Map.get(message, :kafka_published_at_ms)
    })
    {:noreply, socket}
  end

  # Incoming message from another user (1:1 or group)
  def handle_info({:incoming_message, message}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] Session ─▶ WS msg | from=#{message.from} message_id=#{message.id}")
    type = Map.get(message, :type, "1:1")
    push(socket, "msg", %{
      from:                  message.from,
      content:               message.content,
      message_id:            message.id,
      sent_at:               message.sent_at,
      type:                  type,
      conversation_id:       if(type == "group", do: Map.get(message, :conversation_id), else: nil),
      client_sent_at:        Map.get(message, :client_sent_at),
      kafka_published_at_ms: Map.get(message, :kafka_published_at_ms),
      delivered_at_ms:       System.system_time(:millisecond),
      media_url:             Map.get(message, :media_url),
      media_type:            Map.get(message, :media_type)
    })
    {:noreply, socket}
  end

  # Tick-2: recipient device confirmed delivery
  def handle_info({:tick2, message_id}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] Session ─▶ WS tick2 | message_id=#{message_id}")
    push(socket, "tick2", %{message_id: message_id})
    {:noreply, socket}
  end

  # Tick-3: recipient read the message
  def handle_info({:tick3, message_id}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] Session ─▶ WS tick3 | message_id=#{message_id}")
    push(socket, "tick3", %{message_id: message_id})
    {:noreply, socket}
  end

  # Typing indicator received via PubSub — forward to client, skip own events
  def handle_info({:typing, from, conv_id}, socket) do
    if from != socket.assigns.user_id do
      push(socket, "typing", %{from: from, conv_id: conv_id})
    end
    {:noreply, socket}
  end

  # Group invite — push to client so sidebar updates immediately
  def handle_info({:group_invite, group}, socket) do
    push(socket, "group_invite", group)
    # Also subscribe to typing events for the new group
    Phoenix.PubSub.subscribe(MiniWa.PubSub, "typing:group:#{group.group_id}")
    {:noreply, socket}
  end

  # Removed from group — push so sidebar drops it immediately
  def handle_info({:removed_from_group, %{group_id: group_id}}, socket) do
    push(socket, "removed_from_group", %{group_id: group_id})
    Phoenix.PubSub.unsubscribe(MiniWa.PubSub, "typing:group:#{group_id}")
    {:noreply, socket}
  end

  # Presence: another user joined — push their id to our client
  def handle_info({:presence_join, user_id}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] presence_join broadcast received | joined=#{user_id}")
    push(socket, "presence_join", %{user_id: user_id})
    {:noreply, socket}
  end

  # Presence: another user left — push their id to our client
  def handle_info({:presence_leave, user_id}, socket) do
    Logger.info("[Channel][#{socket.assigns.user_id}] presence_leave broadcast received | left=#{user_id}")
    push(socket, "presence_leave", %{user_id: user_id})
    {:noreply, socket}
  end

  @impl true
  def terminate(reason, socket) do
    user_id = socket.assigns.user_id
    Logger.info("[Channel][#{user_id}] Channel terminated | reason=#{inspect(reason)}")
    # Notify all remaining connected clients that this user went offline
    Phoenix.PubSub.broadcast(MiniWa.PubSub, "presence", {:presence_leave, user_id})
    :ok
  end

  # ─── Private ──────────────────────────────────────────────────────────────

  # All registered users (from ScyllaDB) with real-time online flag (from Registry/ETS)
  defp all_users_with_status do
    online = online_user_ids() |> MapSet.new()

    case MiniWa.DB.list_users() do
      {:ok, user_ids} ->
        Enum.map(user_ids, fn uid -> %{user_id: uid, online: MapSet.member?(online, uid)} end)

      {:error, _} ->
        []
    end
  end

  defp online_user_ids do
    Registry.select(MiniWa.Presence.Registry, [{{:"$1", :"$2", :"$3"}, [], [:"$1"]}])
  end

  # Notify a user of a group invite via their Session process (if online)
  defp notify_group_invite(uid, group_id, group_name, invited_by) do
    case Registry.lookup(MiniWa.Presence.Registry, uid) do
      [{pid, _}] -> send(pid, {:group_invite, %{group_id: group_id, name: group_name, invited_by: invited_by}})
      []         -> :offline
    end
  end

  # If a newly-added member is online, push their history share directly
  defp push_history_to_member(uid, group_id, since_ms) do
    case {Registry.lookup(MiniWa.Presence.Registry, uid), MiniWa.DB.fetch_group_history(group_id, since_ms)} do
      {[{pid, _}], {:ok, messages}} ->
        Logger.info("[Channel] Pushing #{length(messages)} historical messages to #{uid}")
        Enum.each(messages, fn m -> GenServer.cast(pid, {:deliver, m}) end)
      _ ->
        :ok
    end
  end
end
