defmodule MiniWa.Analytics.KafkaLag do
  @moduledoc """
  Polls the Kafka broker every 30 s to compute consumer group lag for both
  the delivery group and the analytics group.

  Lag = sum(latest_offset per partition) − sum(committed_offset per partition).
  Stored in Analytics.Store; read by the dashboard JSON endpoint.
  """

  use GenServer
  require Logger

  @poll_ms        30_000
  @first_poll_ms  20_000   # give consumers time to start and commit
  @client         :mini_wa_kafka
  @topic          "messages"
  @num_partitions 4
  @main_group     "mini_wa_consumer_group"
  @analytics_group "mini_wa_analytics_group"

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    Process.send_after(self(), :poll, @first_poll_ms)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:poll, state) do
    poll_lag()
    Process.send_after(self(), :poll, @poll_ms)
    {:noreply, state}
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp poll_lag do
    brokers = Application.get_env(:mini_wa, MiniWa.Streaming, [])
              |> Keyword.get(:kafka_brokers, [{"localhost", 9092}])

    case fetch_latest_total(brokers) do
      nil   -> :ok
      total ->
        record_group_lag(@main_group,      :main,      total)
        record_group_lag(@analytics_group, :analytics, total)
    end
  end

  defp fetch_latest_total(brokers) do
    offsets =
      Enum.map(0..(@num_partitions - 1), fn p ->
        case :brod.resolve_offset(brokers, @topic, p, :latest) do
          {:ok, o} -> o
          _        -> nil
        end
      end)

    if Enum.any?(offsets, &is_nil/1), do: nil, else: Enum.sum(offsets)
  end

  defp record_group_lag(group, atom_key, total_latest) do
    case :brod.fetch_committed_offsets(@client, group) do
      {:ok, topics} ->
        committed = parse_committed_sum(topics)
        lag = max(0, total_latest - committed)
        MiniWa.Analytics.Store.record_kafka_lag(atom_key, lag)
        Logger.debug("[Analytics][KafkaLag] #{group} lag=#{lag}")

      {:error, reason} ->
        Logger.debug("[Analytics][KafkaLag] Could not fetch committed offsets for #{group}: #{inspect(reason)}")
    end
  end

  # brod 3.x returns a list of kpro struct maps.
  # Shape: [%{name: topic, partitions: [%{committed_offset: N, ...}, ...]}, ...]
  defp parse_committed_sum(topics) when is_list(topics) do
    Enum.flat_map(topics, fn topic ->
      partitions = get_in_struct(topic, :partitions, [])
      Enum.map(partitions, fn p -> max(0, get_in_struct(p, :committed_offset, 0)) end)
    end)
    |> Enum.sum()
  end
  defp parse_committed_sum(_), do: 0

  defp get_in_struct(m, key, default) when is_map(m), do: Map.get(m, key, default)
  defp get_in_struct(_, _, default), do: default
end
