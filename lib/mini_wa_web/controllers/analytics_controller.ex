defmodule MiniWaWeb.AnalyticsController do
  use MiniWaWeb, :controller

  def index(conn, _params) do
    conn
    |> put_layout(false)
    |> render(:index)
  end

  def data(conn, _params) do
    json(conn, MiniWa.Cluster.aggregate_analytics())
  end

  def hourly(conn, params) do
    date = Map.get(params, "date", today_string())
    case MiniWa.Analytics.DB.fetch_hourly(date) do
      {:ok, rows} -> json(conn, %{date: date, data: rows})
      _           -> json(conn, %{date: date, data: []})
    end
  end

  def daily(conn, params) do
    month = Map.get(params, "month", month_string())
    case MiniWa.Analytics.DB.fetch_daily(month) do
      {:ok, rows} -> json(conn, %{month: month, data: rows})
      _           -> json(conn, %{month: month, data: []})
    end
  end

  defp today_string, do: Calendar.strftime(DateTime.utc_now(), "%Y-%m-%d")
  defp month_string, do: Calendar.strftime(DateTime.utc_now(), "%Y-%m")
end
