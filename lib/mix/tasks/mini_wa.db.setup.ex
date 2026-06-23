defmodule Mix.Tasks.MiniWa.Db.Setup do
  use Mix.Task

  @shortdoc "Creates the mini_wa keyspace and tables in ScyllaDB"

  # ALTER TABLE statements run separately — they may fail if column already
  # exists, which is fine. We catch and log rather than abort.
  @alter_statements [
    {"messages — add sent_at_ms",
     "ALTER TABLE mini_wa.messages ADD sent_at_ms bigint"},
    {"undelivered_messages — add type",
     "ALTER TABLE mini_wa.undelivered_messages ADD type text"}
  ]

  @statements [
    {"keyspace",
     """
     CREATE KEYSPACE IF NOT EXISTS mini_wa
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
       AND durable_writes = true
     """},
    {"messages table",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.messages (
       conversation_id text,
       message_id      text,
       sender_id       text,
       recipient_id    text,
       content         text,
       status          text,
       sent_at_ms      bigint,
       PRIMARY KEY (conversation_id, message_id)
     ) WITH CLUSTERING ORDER BY (message_id ASC)
     """},
    {"undelivered_messages table",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.undelivered_messages (
       recipient_id    text,
       message_id      text,
       conversation_id text,
       sender_id       text,
       content         text,
       PRIMARY KEY (recipient_id, message_id)
     )
     """},
    {"users table",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.users (
       user_id       text PRIMARY KEY,
       registered_at timestamp
     )
     """},
    {"groups table",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.groups (
       group_id   text PRIMARY KEY,
       name       text,
       created_by text,
       created_at timestamp
     )
     """},
    {"group_members table — partition: group_id (fan-out delivery query)",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.group_members (
       group_id   text,
       user_id    text,
       added_by   text,
       added_at   timestamp,
       share_from bigint,
       PRIMARY KEY (group_id, user_id)
     )
     """},
    {"user_groups table — partition: user_id (sidebar query)",
     """
     CREATE TABLE IF NOT EXISTS mini_wa.user_groups (
       user_id    text,
       group_id   text,
       group_name text,
       PRIMARY KEY (user_id, group_id)
     )
     """}
  ]

  @impl Mix.Task
  def run(_args) do
    Mix.shell().info("Starting Xandra...")
    {:ok, _} = Application.ensure_all_started(:xandra)

    nodes = Application.get_env(:mini_wa, MiniWa.DB, []) |> Keyword.get(:nodes, ["localhost:9042"])
    Mix.shell().info("Connecting to #{inspect(nodes)}...")
    {:ok, conn} = Xandra.start_link(nodes: nodes)

    Mix.shell().info("Running CREATE statements...")
    Enum.each(@statements, fn {label, cql} ->
      case Xandra.execute(conn, String.trim(cql), []) do
        {:ok, _}        -> Mix.shell().info("  ✓ #{label}")
        {:error, error} -> Mix.shell().error("  ✗ #{label}: #{inspect(error)}")
      end
    end)

    Mix.shell().info("Running ALTER statements (errors here are OK if column already exists)...")
    Enum.each(@alter_statements, fn {label, cql} ->
      case Xandra.execute(conn, String.trim(cql), []) do
        {:ok, _}        -> Mix.shell().info("  ✓ #{label}")
        {:error, error} -> Mix.shell().info("  ~ #{label}: #{inspect(error)}")
      end
    end)

    Mix.shell().info("""

    Schema ready:
      mini_wa.users                — registered users
      mini_wa.messages             — durable log (partition: conversation_id, has sent_at_ms)
      mini_wa.undelivered_messages — offline queue (partition: recipient_id)
      mini_wa.groups               — group metadata
      mini_wa.group_members        — membership (partition: group_id)
      mini_wa.user_groups          — user→group index (partition: user_id)
    """)

    GenServer.stop(conn)
  end
end
