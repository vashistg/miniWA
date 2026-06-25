defmodule MiniWaWeb.Router do
  use MiniWaWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {MiniWaWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", MiniWaWeb do
    pipe_through :browser

    get "/", PageController, :home
  end

  scope "/api", MiniWaWeb do
    pipe_through :api
    post "/upload",           UploadController,    :create
    get  "/analytics",        AnalyticsController, :data
    get  "/analytics/hourly", AnalyticsController, :hourly
    get  "/analytics/daily",  AnalyticsController, :daily
    post "/agent/chat",       AgentController,     :chat
  end

  scope "/", MiniWaWeb do
    pipe_through :browser
    get "/analytics", AnalyticsController, :index
  end
end
