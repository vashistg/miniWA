defmodule MiniWaWeb.PageController do
  use MiniWaWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
