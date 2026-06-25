defmodule MiniWa.Agent.Tools do
  @max_rows 100

  def definitions do
    [
      %{
        "type" => "function",
        "function" => %{
          "name" => "query_db",
          "description" => """
          Run a read-only SELECT query against ScyllaDB. Only SELECT statements are allowed.
          A LIMIT #{@max_rows} is automatically appended if none is present.

          Available tables:
          - mini_wa.messages          PK (conversation_id, message_id) — message_id is time-ordered hex
          - mini_wa.users             PK user_id
          - mini_wa.groups            PK group_id
          - mini_wa.group_members     PK (group_id, user_id)
          - mini_wa.user_groups       PK (user_id, group_id)
          - mini_wa.undelivered_messages  PK (recipient_id, message_id)
          - mini_wa.analytics_counters    PK name — lifetime counters (total, text_c, image_c, audio_c, video_c, lat_sum, lat_cnt, session_starts, session_crashes)
          - mini_wa.analytics_hourly  PK day (YYYY-MM-DD), clustering hour (0-23)
          - mini_wa.analytics_daily   PK month (YYYY-MM), clustering day (YYYY-MM-DD)

          Notes:
          - conversation_id for 1:1: sorted join e.g. "alice:bob"; for groups: UUID string
          - Add ALLOW FILTERING when filtering on non-partition-key columns
          - analytics counters are COUNTER type (bigint); divide lat_sum/lat_cnt for avg latency
          """,
          "parameters" => %{
            "type" => "object",
            "properties" => %{
              "query" => %{"type" => "string", "description" => "CQL SELECT statement to execute"}
            },
            "required" => ["query"]
          }
        }
      },
      %{
        "type" => "function",
        "function" => %{
          "name" => "create_html_file",
          "description" => """
          Create a standalone HTML file that will be served and shown as an inline preview.
          Use this to generate charts, dashboards, tables, and any other visual output.

          Always use this visual style to match the miniWA dark theme:
          - Page/body background: #0f1117
          - Card background: #1a1f2e
          - Text: #e2e8f0
          - Border color: #2d3748
          - Accent green: #25D366 | Blue: #4299e1 | Orange: #ed8936 | Teal: #38b2ac | Yellow: #ecc94b
          - Load Plotly.js from: https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js
          - Use Plotly for all charts. Set layout paper_bgcolor and plot_bgcolor to '#0f1117', font color to '#e2e8f0', gridcolor to '#2d3748'. Use the accent colors above for traces.
          - Prefer interactive chart types: bar, scatter, line, pie, sunburst, treemap, heatmap, sankey, box, histogram. Add titles, axis labels, and hover templates.
          - Embed all data inline — the file has no access to the Phoenix server

          After this tool returns, tell the user the filename.
          """,
          "parameters" => %{
            "type" => "object",
            "properties" => %{
              "filename" => %{"type" => "string", "description" => "Filename ending in .html, e.g. 'dashboard.html'"},
              "content" => %{"type" => "string", "description" => "Complete self-contained HTML content"}
            },
            "required" => ["filename", "content"]
          }
        }
      },
      %{
        "type" => "function",
        "function" => %{
          "name" => "read_file",
          "description" => "Read the content of a previously created agent file.",
          "parameters" => %{
            "type" => "object",
            "properties" => %{
              "filename" => %{"type" => "string", "description" => "Filename to read"}
            },
            "required" => ["filename"]
          }
        }
      },
      %{
        "type" => "function",
        "function" => %{
          "name" => "list_agent_files",
          "description" => "List all HTML files previously created in this session.",
          "parameters" => %{
            "type" => "object",
            "properties" => %{},
            "required" => []
          }
        }
      }
    ]
  end

  def anthropic_definitions do
    Enum.map(definitions(), fn %{"function" => f} ->
      %{
        "name" => f["name"],
        "description" => f["description"],
        "input_schema" => f["parameters"]
      }
    end)
  end

  # Returns {output_string, [%{filename: name, url: url}]}
  def execute("query_db", %{"query" => raw_query}) do
    query = String.trim(raw_query)

    cond do
      not String.match?(query, ~r/^select\s/i) ->
        {encode_error("Only SELECT queries are allowed"), []}

      String.contains?(query, ";") ->
        {encode_error("Semicolons are not allowed"), []}

      true ->
        q = if String.match?(query, ~r/\blimit\b/i), do: query, else: query <> " LIMIT #{@max_rows}"

        case Xandra.execute(MiniWa.Xandra, q, []) do
          {:ok, page} ->
            rows = Enum.map(page, fn row ->
              Map.new(row, fn {k, v} -> {k, format_value(v)} end)
            end)
            {Jason.encode!(%{rows: rows, count: length(rows)}), []}

          {:error, e} ->
            {encode_error(inspect(e)), []}
        end
    end
  rescue
    e -> {encode_error(inspect(e)), []}
  end

  def execute("create_html_file", %{"filename" => raw_name, "content" => content}) do
    name =
      raw_name
      |> String.replace(~r/[^a-zA-Z0-9\-_\.]/, "_")
      |> String.slice(0, 64)

    name = if String.ends_with?(name, ".html"), do: name, else: name <> ".html"

    path = Path.join(files_dir(), name)
    File.write!(path, content)

    url = "/agent_files/#{name}"
    result = %{filename: name, url: url}
    {Jason.encode!(result), [%{filename: name, url: url}]}
  rescue
    e -> {encode_error(inspect(e)), []}
  end

  def execute("read_file", %{"filename" => raw_name}) do
    name =
      raw_name
      |> String.replace(~r/[^a-zA-Z0-9\-_\.]/, "_")
      |> String.slice(0, 64)

    path = Path.join(files_dir(), name)

    case File.read(path) do
      {:ok, content} -> {content, []}
      {:error, :enoent} -> {encode_error("File not found: #{name}"), []}
      {:error, e} -> {encode_error(inspect(e)), []}
    end
  rescue
    e -> {encode_error(inspect(e)), []}
  end

  def execute("list_agent_files", _input) do
    files =
      case File.ls(files_dir()) do
        {:ok, names} -> names |> Enum.filter(&String.ends_with?(&1, ".html")) |> Enum.sort()
        _ -> []
      end

    {Jason.encode!(%{files: files}), []}
  rescue
    e -> {encode_error(inspect(e)), []}
  end

  def execute(unknown, _input) do
    {encode_error("Unknown tool: #{unknown}"), []}
  end

  # ─── Private ────────────────────────────────────────────────────────────────

  defp files_dir do
    dir = Path.join([:code.priv_dir(:mini_wa), "static", "agent_files"])
    File.mkdir_p!(dir)
    dir
  end

  defp encode_error(msg), do: Jason.encode!(%{error: msg})

  defp format_value(nil), do: nil
  defp format_value(v) when is_binary(v), do: v
  defp format_value(v) when is_number(v), do: v
  defp format_value(v) when is_boolean(v), do: v
  defp format_value(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
  defp format_value(v), do: inspect(v)
end
