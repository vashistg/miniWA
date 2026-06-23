defmodule MiniWa.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    require Logger

    Logger.info("""
    [App] ══════════════════════════════════════════════════
      miniWA starting up
      Supervisor tree:
        MiniWa.Presence.Registry    — ETS-backed presence (user_id → Session PID)
        MiniWa.Session.Supervisor   — DynamicSupervisor (one Session per user)
        MiniWa.Streaming.Consumer   — Kafka consumer → ScyllaDB + delivery
        MiniWaWeb.Endpoint          — Phoenix WebSocket endpoint
    ══════════════════════════════════════════════════════
    """)

    kafka_brokers = Application.get_env(:mini_wa, MiniWa.Streaming, [])
                    |> Keyword.get(:kafka_brokers, [{"localhost", 9092}])

    children = [
      MiniWaWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:mini_wa, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: MiniWa.PubSub},
      # Xandra connection pool → ScyllaDB
      {Xandra,
       nodes: Application.get_env(:mini_wa, MiniWa.DB, []) |> Keyword.get(:nodes, ["localhost:9042"]),
       name: MiniWa.Xandra},
      # Presence Registry: maps user_id → Session PID via ETS.
      {Registry, keys: :unique, name: MiniWa.Presence.Registry},
      # DynamicSupervisor: one Session GenServer per connected user.
      {DynamicSupervisor, name: MiniWa.Session.Supervisor, strategy: :one_for_one},
      # brod Kafka client — connection pool to the broker.
      # auto_start_producers: true lets us call produce_sync without manually
      # registering each topic first.
      %{
        id: :brod_client,
        start: {:brod, :start_link_client, [
          kafka_brokers,
          :mini_wa_kafka,
          [auto_start_producers: true, default_producer_config: []]
        ]},
        type: :worker,
        restart: :permanent
      },
      # Kafka consumer — reads messages topic, writes ScyllaDB, delivers to Sessions
      MiniWa.Streaming.Consumer,
      MiniWaWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: MiniWa.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    MiniWaWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
