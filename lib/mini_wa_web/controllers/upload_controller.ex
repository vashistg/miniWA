defmodule MiniWaWeb.UploadController do
  use MiniWaWeb, :controller
  require Logger

  def create(conn, %{"file" => %Plug.Upload{} = upload}) do
    case MiniWa.Media.upload(upload) do
      {:ok, url, media_type} ->
        json(conn, %{url: url, media_type: media_type})

      {:error, reason} ->
        conn |> put_status(400) |> json(%{error: reason})
    end
  end

  def create(conn, _params) do
    conn |> put_status(400) |> json(%{error: "no file attached"})
  end
end
