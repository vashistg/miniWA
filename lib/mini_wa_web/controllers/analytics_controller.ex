defmodule MiniWaWeb.AnalyticsController do
  use MiniWaWeb, :controller

  def index(conn, _params) do
    conn
    |> put_layout(false)
    |> render(:index)
  end

  def data(conn, _params) do
    snapshot = MiniWa.Analytics.Store.get_snapshot()
    json(conn, snapshot)
  end
end
