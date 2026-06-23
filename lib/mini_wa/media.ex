defmodule MiniWa.Media do
  require Logger

  @max_bytes 50 * 1024 * 1024

  defp bucket,   do: Application.get_env(:mini_wa, __MODULE__, []) |> Keyword.get(:bucket, "mini-wa-media")
  defp base_url, do: Application.get_env(:mini_wa, __MODULE__, []) |> Keyword.get(:base_url, "http://localhost:9000")

  @upload_chunk_size 5 * 1024 * 1024  # 5 MB chunks for multipart upload

  # Upload a file from a Plug.Upload to MinIO. Returns {:ok, public_url, media_type} | {:error, reason}.
  # Uses ExAws streaming multipart upload so the file is never fully loaded into memory.
  def upload(%Plug.Upload{path: path, filename: orig, content_type: content_type}) do
    with :ok <- check_size(path),
         {:ok, media_type} <- detect_type(content_type) do
      ext = Path.extname(orig) |> String.downcase()
      key = "#{:crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)}#{ext}"

      result =
        path
        |> ExAws.S3.Upload.stream_file(chunk_size: @upload_chunk_size)
        |> ExAws.S3.upload(bucket(), key,
          content_type: content_type,
          timeout: 300_000
        )
        |> ExAws.request()

      case result do
        {:ok, _} ->
          url = "#{base_url()}/#{bucket()}/#{key}"
          Logger.info("[Media] uploaded #{media_type} #{key}")
          {:ok, url, media_type}

        {:error, reason} ->
          Logger.error("[Media] MinIO upload failed: #{inspect(reason)}")
          {:error, "upload failed — is MinIO running?"}
      end
    end
  end

  # Create the bucket (idempotent) and set a public-read policy.
  # Called from the mix mini_wa.db.setup task.
  def setup! do
    b = bucket()
    case ExAws.S3.put_bucket(b, "us-east-1") |> ExAws.request() do
      {:ok, _} ->
        Logger.info("[Media] bucket '#{b}' created")
        set_public_policy(b)

      {:error, {:http_error, 409, _}} ->
        Logger.info("[Media] bucket '#{b}' already exists")
        set_public_policy(b)

      {:error, reason} ->
        raise "MinIO bucket setup failed: #{inspect(reason)}"
    end
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp set_public_policy(b) do
    policy = Jason.encode!(%{
      "Version" => "2012-10-17",
      "Statement" => [%{
        "Effect"    => "Allow",
        "Principal" => %{"AWS" => ["*"]},
        "Action"    => ["s3:GetObject"],
        "Resource"  => ["arn:aws:s3:::#{b}/*"]
      }]
    })
    ExAws.S3.put_bucket_policy(b, policy) |> ExAws.request!()
    Logger.info("[Media] public-read policy applied to bucket '#{b}'")
  end

  defp check_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} when size <= @max_bytes -> :ok
      {:ok, _} -> {:error, "file too large (max 50 MB)"}
      _        -> :ok
    end
  end

  defp detect_type(content_type) do
    cond do
      String.starts_with?(content_type || "", "image/") -> {:ok, "image"}
      String.starts_with?(content_type || "", "audio/") -> {:ok, "audio"}
      String.starts_with?(content_type || "", "video/") -> {:ok, "video"}
      true -> {:error, "unsupported file type — only image, audio, video allowed"}
    end
  end
end
