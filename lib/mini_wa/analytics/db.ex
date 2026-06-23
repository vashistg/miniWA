defmodule MiniWa.Analytics.DB do
  @moduledoc """
  ScyllaDB persistence for analytics data.

  Three counter tables:
    analytics_counters — global lifetime totals (one row per metric name)
    analytics_hourly   — hourly breakdown (partition: day, cluster: hour)
    analytics_daily    — daily breakdown  (partition: month, cluster: day)

  All writes are fire-and-forget (callers spawn tasks). Every function
  swallows errors so a ScyllaDB hiccup never affects the hot path.
  """

  @conn MiniWa.Xandra

  # ─── Write ─────────────────────────────────────────────────────────────────

  def record_message(media_type, latency_ms) do
    {day, hour, month} = time_keys()
    col = media_column(media_type)
    {lat_part, lat_params} = lat_query_parts(latency_ms)

    bump_global("total", 1)
    bump_global(col, 1)

    if is_integer(latency_ms) && latency_ms >= 0 do
      bump_global("lat_sum", latency_ms)
      bump_global("lat_cnt", 1)
    end

    Xandra.execute(@conn,
      "UPDATE mini_wa.analytics_hourly SET total = total + 1, #{col} = #{col} + 1#{lat_part} WHERE day = ? AND hour = ?",
      lat_params ++ [{"text", day}, {"int", hour}])

    Xandra.execute(@conn,
      "UPDATE mini_wa.analytics_daily SET total = total + 1, #{col} = #{col} + 1#{lat_part} WHERE month = ? AND day = ?",
      lat_params ++ [{"text", month}, {"text", day}])

    :ok
  rescue
    _ -> :ok
  end

  def record_session_event(type) do
    name = if type == :start, do: "session_starts", else: "session_crashes"
    bump_global(name, 1)
    :ok
  end

  # ─── Read ───────────────────────────────────────────────────────────────────

  # Returns a map of %{"total" => N, "image_c" => N, ...} for seeding ETS on startup.
  def load_counters do
    case Xandra.execute(@conn, "SELECT name, value FROM mini_wa.analytics_counters", []) do
      {:ok, page} ->
        Enum.reduce(page, %{}, fn row, acc -> Map.put(acc, row["name"], row["value"] || 0) end)
      _ ->
        %{}
    end
  rescue
    _ -> %{}
  end

  def fetch_hourly(day) do
    case Xandra.execute(@conn,
      "SELECT hour, total, text_c, image_c, audio_c, video_c, lat_sum, lat_cnt FROM mini_wa.analytics_hourly WHERE day = ?",
      [{"text", day}]) do
      {:ok, page} -> {:ok, Enum.map(page, &normalize/1)}
      {:error, e} -> {:error, e}
    end
  rescue
    _ -> {:ok, []}
  end

  def fetch_daily(month) do
    case Xandra.execute(@conn,
      "SELECT day, total, text_c, image_c, audio_c, video_c, lat_sum, lat_cnt FROM mini_wa.analytics_daily WHERE month = ?",
      [{"text", month}]) do
      {:ok, page} -> {:ok, Enum.map(page, &normalize/1)}
      {:error, e} -> {:error, e}
    end
  rescue
    _ -> {:ok, []}
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp bump_global(name, delta) do
    Xandra.execute(@conn,
      "UPDATE mini_wa.analytics_counters SET value = value + ? WHERE name = ?",
      [{"bigint", delta}, {"text", name}])
  rescue
    _ -> :ok
  end

  # col is always one of the four hardcoded strings from media_column/1 — safe to interpolate.
  defp lat_query_parts(ms) when is_integer(ms) and ms >= 0 do
    {", lat_sum = lat_sum + ?, lat_cnt = lat_cnt + ?", [{"bigint", ms}, {"bigint", 1}]}
  end
  defp lat_query_parts(_), do: {"", []}

  defp time_keys do
    now = DateTime.utc_now()
    {
      Calendar.strftime(now, "%Y-%m-%d"),
      now.hour,
      Calendar.strftime(now, "%Y-%m")
    }
  end

  defp media_column("image"), do: "image_c"
  defp media_column("audio"), do: "audio_c"
  defp media_column("video"), do: "video_c"
  defp media_column(_),       do: "text_c"

  # Scylla counters decode as integers; replace nils with 0.
  defp normalize(row), do: Map.new(row, fn {k, v} -> {k, v || 0} end)
end
