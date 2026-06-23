defmodule MiniWa.Cluster do
  @moduledoc """
  Cluster-aware helpers for session discovery and analytics aggregation.

  Session lookup uses Erlang's :pg (process groups), which gossips membership
  across all connected nodes automatically. Once we have a PID — local or
  remote — GenServer.cast/send work identically: Erlang's distribution layer
  serializes and routes the message transparently.

  The local Registry (MiniWa.Presence.Registry) is kept for the Session's own
  registered name ({:via, Registry, ...}) so intra-node calls still use ETS
  speed. :pg is only for cross-node lookup.
  """

  @scope MiniWa.SessionGroup

  # ─── Session discovery ────────────────────────────────────────────────────

  @doc "Returns {:ok, pid} if the user has an active Session on any node, else :not_found."
  def find_session(user_id) do
    case :pg.get_members(@scope, user_id) do
      [pid | _] -> {:ok, pid}
      []        -> :not_found
    end
  end

  @doc "True if the user has an active Session on any node in the cluster."
  def online?(user_id) do
    :pg.get_members(@scope, user_id) != []
  end

  @doc "Returns the list of user_ids with an active Session across the whole cluster."
  def online_users do
    :pg.which_groups(@scope)
  end

  # ─── Analytics aggregation ────────────────────────────────────────────────

  @doc """
  Collects snapshots from every node and merges them into a single map.
  Counters are summed; latency samples are concatenated (capped for UI);
  rate buckets are merged by minute bucket.
  """
  def aggregate_analytics do
    nodes = [Node.self() | Node.list()]

    snapshots =
      Enum.flat_map(nodes, fn node ->
        case :rpc.call(node, MiniWa.Analytics.Store, :get_snapshot, []) do
          {:badrpc, _} -> []
          snap         -> [snap]
        end
      end)

    case snapshots do
      []    -> MiniWa.Analytics.Store.get_snapshot()
      [one] -> one
      many  -> merge_snapshots(many)
    end
  end

  # ─── Private ──────────────────────────────────────────────────────────────

  defp merge_snapshots(snaps) do
    base = %{
      total: 0, text: 0, image: 0, audio: 0, video: 0,
      session_starts: 0, session_crashes: 0,
      active_sessions: 0,
      kafka_lag_main: nil, kafka_lag_analytics: nil,
      latency: %{p50: nil, p95: nil, p99: nil, mean: nil, count: 0, samples: []},
      rate: %{per_minute: [], current_rpm: 0}
    }

    Enum.reduce(snaps, base, fn snap, acc ->
      acc
      |> Map.update!(:total,           &(&1 + Map.get(snap, :total, 0)))
      |> Map.update!(:text,            &(&1 + Map.get(snap, :text, 0)))
      |> Map.update!(:image,           &(&1 + Map.get(snap, :image, 0)))
      |> Map.update!(:audio,           &(&1 + Map.get(snap, :audio, 0)))
      |> Map.update!(:video,           &(&1 + Map.get(snap, :video, 0)))
      |> Map.update!(:session_starts,  &(&1 + Map.get(snap, :session_starts, 0)))
      |> Map.update!(:session_crashes, &(&1 + Map.get(snap, :session_crashes, 0)))
      |> Map.update!(:active_sessions, &(&1 + Map.get(snap, :active_sessions, 0)))
      |> merge_lag(snap)
      |> merge_latency(Map.get(snap, :latency, %{}))
      |> merge_rate(Map.get(snap, :rate, %{}))
    end)
  end

  defp merge_lag(acc, snap) do
    acc
    |> Map.update!(:kafka_lag_main,      &(pick_lag(&1, snap[:kafka_lag_main])))
    |> Map.update!(:kafka_lag_analytics, &(pick_lag(&1, snap[:kafka_lag_analytics])))
  end

  defp pick_lag(nil, b), do: b
  defp pick_lag(a, nil), do: a
  defp pick_lag(a, b),   do: a + b

  defp merge_latency(acc, node_lat) do
    node_samples = Map.get(node_lat, :samples, [])
    Map.update!(acc, :latency, fn lat ->
      all_samples = Enum.take(lat.samples ++ node_samples, 500)
      if all_samples == [] do
        lat
      else
        sorted = Enum.sort(all_samples)
        n      = length(sorted)
        %{lat |
          count:   lat.count + Map.get(node_lat, :count, 0),
          samples: all_samples,
          p50:     percentile(sorted, n, 0.50),
          p95:     percentile(sorted, n, 0.95),
          p99:     percentile(sorted, n, 0.99),
          mean:    round(Enum.sum(sorted) / n)
        }
      end
    end)
  end

  defp merge_rate(acc, node_rate) do
    node_buckets =
      Map.get(node_rate, :per_minute, [])
      |> Enum.map(fn %{minutes_ago: m, count: c} -> {m, c} end)
      |> Map.new()

    Map.update!(acc, :rate, fn rate ->
      existing =
        Map.get(rate, :per_minute, [])
        |> Enum.map(fn %{minutes_ago: m, count: c} -> {m, c} end)
        |> Map.new()

      merged =
        Map.merge(existing, node_buckets, fn _k, c1, c2 -> c1 + c2 end)
        |> Enum.sort_by(fn {m, _} -> m end)
        |> Enum.map(fn {m, c} -> %{minutes_ago: m, count: c} end)

      current_rpm = Enum.sum(for %{minutes_ago: m, count: c} <- merged, m <= 1, do: c)

      %{rate | per_minute: merged, current_rpm: current_rpm}
    end)
  end

  defp percentile(sorted, n, pct) do
    Enum.at(sorted, max(0, round(pct * n) - 1))
  end
end
