defmodule MiniWa.Analytics.Store do
  use GenServer

  @table :analytics_store
  @max_latency_samples 500
  @rate_window_minutes 60

  # ─── Public API ────────────────────────────────────────────────────────────

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  # Called by the analytics Kafka consumer for every message.
  # ETS counters are updated synchronously (atomic, microseconds).
  # ScyllaDB write is fire-and-forget in a separate task.
  def record_message(media_type, latency_ms \\ nil) do
    safe_counter(:total_messages)
    case media_type do
      "image" -> safe_counter(:media_image)
      "audio" -> safe_counter(:media_audio)
      "video" -> safe_counter(:media_video)
      _       -> safe_counter(:text_only)
    end
    if is_integer(latency_ms) && latency_ms >= 0 do
      GenServer.cast(__MODULE__, {:add_latency, latency_ms})
    end
    GenServer.cast(__MODULE__, {:bump_rate, System.system_time(:millisecond)})
    Task.start(fn -> MiniWa.Analytics.DB.record_message(media_type, latency_ms) end)
  end

  # Called from Session.init/1 — direct ETS + async ScyllaDB.
  def record_session_start do
    safe_counter(:session_starts)
    Task.start(fn -> MiniWa.Analytics.DB.record_session_event(:start) end)
  end

  # Called from Session.terminate/2 on non-normal exits.
  def record_crash do
    safe_counter(:session_crashes)
    Task.start(fn -> MiniWa.Analytics.DB.record_session_event(:crash) end)
  end

  def record_kafka_lag(group, lag) when is_integer(lag) do
    key = if group == :main, do: :kafka_lag_main, else: :kafka_lag_analytics
    safe_insert(key, lag)
  end

  def get_snapshot do
    GenServer.call(__MODULE__, :snapshot)
  end

  # ─── GenServer callbacks ────────────────────────────────────────────────────

  @impl true
  def init(_) do
    :ets.new(@table, [:named_table, :public, :set])
    :ets.insert(@table, [
      {:total_messages,    0},
      {:media_image,       0},
      {:media_audio,       0},
      {:media_video,       0},
      {:text_only,         0},
      {:session_starts,    0},
      {:session_crashes,   0},
      {:kafka_lag_main,      nil},
      {:kafka_lag_analytics, nil},
      {:latency_samples,   []},
      {:rate_buckets,      %{}}
    ])
    seed_from_scylla()
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:add_latency, ms}, state) do
    [{_, samples}] = :ets.lookup(@table, :latency_samples)
    :ets.insert(@table, {:latency_samples, Enum.take([ms | samples], @max_latency_samples)})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:bump_rate, ts_ms}, state) do
    bucket  = div(ts_ms, 60_000)
    [{_, buckets}] = :ets.lookup(@table, :rate_buckets)
    updated = Map.update(buckets, bucket, 1, &(&1 + 1))
    cutoff  = bucket - @rate_window_minutes
    :ets.insert(@table, {:rate_buckets, Map.reject(updated, fn {k, _} -> k < cutoff end)})
    {:noreply, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    snapshot = %{
      total:           ets_val(:total_messages),
      image:           ets_val(:media_image),
      audio:           ets_val(:media_audio),
      video:           ets_val(:media_video),
      text:            ets_val(:text_only),
      session_starts:  ets_val(:session_starts),
      session_crashes: ets_val(:session_crashes),
      kafka_lag_main:      ets_val(:kafka_lag_main),
      kafka_lag_analytics: ets_val(:kafka_lag_analytics),
      active_sessions: Registry.count(MiniWa.Presence.Registry),
      latency:         compute_latency_stats(ets_val(:latency_samples)),
      rate:            compute_rate(ets_val(:rate_buckets))
    }
    {:reply, snapshot, state}
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  # Seed ETS counters from ScyllaDB so they survive server restarts.
  defp seed_from_scylla do
    persisted = MiniWa.Analytics.DB.load_counters()
    unless map_size(persisted) == 0 do
      :ets.insert(@table, [
        {:total_messages,  Map.get(persisted, "total",           0)},
        {:media_image,     Map.get(persisted, "image_c",         0)},
        {:media_audio,     Map.get(persisted, "audio_c",         0)},
        {:media_video,     Map.get(persisted, "video_c",         0)},
        {:text_only,       Map.get(persisted, "text_c",          0)},
        {:session_starts,  Map.get(persisted, "session_starts",  0)},
        {:session_crashes, Map.get(persisted, "session_crashes", 0)}
      ])
    end
  rescue
    _ -> :ok
  end

  defp safe_counter(key) do
    :ets.update_counter(@table, key, 1)
  rescue
    _ -> :ok
  end

  defp safe_insert(key, val) do
    :ets.insert(@table, {key, val})
  rescue
    _ -> :ok
  end

  defp ets_val(key) do
    case :ets.lookup(@table, key) do
      [{_, v}] -> v
      []       -> nil
    end
  end

  defp compute_latency_stats([]) do
    %{p50: nil, p95: nil, p99: nil, mean: nil, count: 0, samples: []}
  end
  defp compute_latency_stats(samples) do
    sorted = Enum.sort(samples)
    n      = length(sorted)
    %{
      p50:     percentile(sorted, n, 0.50),
      p95:     percentile(sorted, n, 0.95),
      p99:     percentile(sorted, n, 0.99),
      mean:    round(Enum.sum(sorted) / n),
      count:   n,
      samples: Enum.take(Enum.reverse(samples), 100)
    }
  end

  defp percentile(sorted, n, pct) do
    Enum.at(sorted, max(0, round(pct * n) - 1))
  end

  defp compute_rate(buckets) when map_size(buckets) == 0 do
    %{per_minute: [], current_rpm: 0}
  end
  defp compute_rate(buckets) do
    now_bucket = div(System.system_time(:millisecond), 60_000)
    per_minute =
      buckets
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.map(fn {bucket, count} -> %{minutes_ago: now_bucket - bucket, count: count} end)
    current_rpm = Map.get(buckets, now_bucket, 0) + Map.get(buckets, now_bucket - 1, 0)
    %{per_minute: per_minute, current_rpm: current_rpm}
  end
end
