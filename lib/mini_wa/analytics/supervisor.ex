defmodule MiniWa.Analytics.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      MiniWa.Analytics.Store,
      MiniWa.Analytics.Consumer,
      MiniWa.Analytics.KafkaLag
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
