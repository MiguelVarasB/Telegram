BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "active_stories" (
	"dialog_id"	INT8,
	"story_list_id"	INT4,
	"dialog_order"	INT8,
	"data"	BLOB,
	PRIMARY KEY("dialog_id")
);
CREATE TABLE IF NOT EXISTS "active_story_lists" (
	"story_list_id"	INT4,
	"data"	BLOB,
	PRIMARY KEY("story_list_id")
);
CREATE TABLE IF NOT EXISTS "common" (
	"k"	BLOB,
	"v"	BLOB,
	PRIMARY KEY("k")
);
CREATE TABLE IF NOT EXISTS "dialogs" (
	"dialog_id"	INT8,
	"dialog_order"	INT8,
	"data"	BLOB,
	"folder_id"	INT4,
	PRIMARY KEY("dialog_id")
);
CREATE TABLE IF NOT EXISTS "encryption_dummy_table" (
	"id"	INT,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "files" (
	"k"	BLOB,
	"v"	BLOB,
	PRIMARY KEY("k")
);
CREATE TABLE IF NOT EXISTS "messages" (
	"dialog_id"	INT8,
	"message_id"	INT8,
	"unique_message_id"	INT4,
	"sender_user_id"	INT8,
	"random_id"	INT8,
	"data"	BLOB,
	"ttl_expires_at"	INT4,
	"index_mask"	INT4,
	"search_id"	INT8,
	"text"	STRING,
	"notification_id"	INT4,
	"top_thread_message_id"	INT8,
	PRIMARY KEY("dialog_id","message_id")
);
CREATE TABLE IF NOT EXISTSAL TABLE messages_fts USING fts5(text, content='messages', content_rowid='search_id', tokenize = "unicode61 remove_diacritics 0 tokenchars ''");
CREATE TABLE IF NOT EXISTS "messages_fts_config" (
	"k"	,
	"v"	,
	PRIMARY KEY("k")
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS "messages_fts_data" (
	"id"	INTEGER,
	"block"	BLOB,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "messages_fts_docsize" (
	"id"	INTEGER,
	"sz"	BLOB,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "messages_fts_idx" (
	"segid"	,
	"term"	,
	"pgno"	,
	PRIMARY KEY("segid","term")
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS "notification_groups" (
	"notification_group_id"	INT4,
	"dialog_id"	INT8,
	"last_notification_date"	INT4,
	PRIMARY KEY("notification_group_id")
);
CREATE TABLE IF NOT EXISTS "scheduled_messages" (
	"dialog_id"	INT8,
	"message_id"	INT8,
	"server_message_id"	INT4,
	"data"	BLOB,
	PRIMARY KEY("dialog_id","message_id")
);
CREATE TABLE IF NOT EXISTS "stories" (
	"dialog_id"	INT8,
	"story_id"	INT4,
	"expires_at"	INT4,
	"notification_id"	INT4,
	"data"	BLOB,
	PRIMARY KEY("dialog_id","story_id")
);
CREATE INDEX IF NOT EXISTS "active_stories_by_order" ON "active_stories" (
	"story_list_id",
	"dialog_order",
	"dialog_id"
) WHERE "story_list_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "dialog_in_folder_by_dialog_order" ON "dialogs" (
	"folder_id",
	"dialog_order",
	"dialog_id"
) WHERE "folder_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "full_message_index_10" ON "messages" (
	"unique_message_id"
) WHERE ("index_mask" & 1024) != 0;
CREATE INDEX IF NOT EXISTS "full_message_index_9" ON "messages" (
	"unique_message_id"
) WHERE ("index_mask" & 512) != 0;
CREATE INDEX IF NOT EXISTS "message_by_notification_id" ON "messages" (
	"dialog_id",
	"notification_id"
) WHERE "notification_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_by_random_id" ON "messages" (
	"dialog_id",
	"random_id"
) WHERE "random_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_by_search_id" ON "messages" (
	"search_id"
) WHERE "search_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_by_server_message_id" ON "scheduled_messages" (
	"dialog_id",
	"server_message_id"
) WHERE "server_message_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_by_ttl" ON "messages" (
	"ttl_expires_at"
) WHERE "ttl_expires_at" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_by_unique_message_id" ON "messages" (
	"unique_message_id"
) WHERE "unique_message_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "message_index_0" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 1) != 0;
CREATE INDEX IF NOT EXISTS "message_index_1" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 2) != 0;
CREATE INDEX IF NOT EXISTS "message_index_10" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 1024) != 0;
CREATE INDEX IF NOT EXISTS "message_index_11" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 2048) != 0;
CREATE INDEX IF NOT EXISTS "message_index_12" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 4096) != 0;
CREATE INDEX IF NOT EXISTS "message_index_13" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 8192) != 0;
CREATE INDEX IF NOT EXISTS "message_index_14" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 16384) != 0;
CREATE INDEX IF NOT EXISTS "message_index_15" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 32768) != 0;
CREATE INDEX IF NOT EXISTS "message_index_16" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 65536) != 0;
CREATE INDEX IF NOT EXISTS "message_index_17" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 131072) != 0;
CREATE INDEX IF NOT EXISTS "message_index_18" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 262144) != 0;
CREATE INDEX IF NOT EXISTS "message_index_19" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 524288) != 0;
CREATE INDEX IF NOT EXISTS "message_index_2" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 4) != 0;
CREATE INDEX IF NOT EXISTS "message_index_20" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 1048576) != 0;
CREATE INDEX IF NOT EXISTS "message_index_21" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 2097152) != 0;
CREATE INDEX IF NOT EXISTS "message_index_22" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 4194304) != 0;
CREATE INDEX IF NOT EXISTS "message_index_23" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 8388608) != 0;
CREATE INDEX IF NOT EXISTS "message_index_24" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 16777216) != 0;
CREATE INDEX IF NOT EXISTS "message_index_25" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 33554432) != 0;
CREATE INDEX IF NOT EXISTS "message_index_26" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 67108864) != 0;
CREATE INDEX IF NOT EXISTS "message_index_27" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 134217728) != 0;
CREATE INDEX IF NOT EXISTS "message_index_28" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 268435456) != 0;
CREATE INDEX IF NOT EXISTS "message_index_29" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 536870912) != 0;
CREATE INDEX IF NOT EXISTS "message_index_3" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 8) != 0;
CREATE INDEX IF NOT EXISTS "message_index_4" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 16) != 0;
CREATE INDEX IF NOT EXISTS "message_index_5" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 32) != 0;
CREATE INDEX IF NOT EXISTS "message_index_6" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 64) != 0;
CREATE INDEX IF NOT EXISTS "message_index_7" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 128) != 0;
CREATE INDEX IF NOT EXISTS "message_index_8" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 256) != 0;
CREATE INDEX IF NOT EXISTS "message_index_9" ON "messages" (
	"dialog_id",
	"message_id"
) WHERE ("index_mask" & 512) != 0;
CREATE INDEX IF NOT EXISTS "notification_group_by_last_notification_date" ON "notification_groups" (
	"last_notification_date",
	"dialog_id",
	"notification_group_id"
) WHERE "last_notification_date" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "story_by_notification_id" ON "stories" (
	"dialog_id",
	"notification_id"
) WHERE "notification_id" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "story_by_ttl" ON "stories" (
	"expires_at"
) WHERE "expires_at" IS NOT NULL;
CREATE TRIGGER trigger_fts_delete BEFORE DELETE ON messages WHEN OLD.search_id IS NOT NULL BEGIN INSERT INTO messages_fts(messages_fts, rowid, text) VALUES('delete', OLD.search_id, OLD.text); END;
CREATE TRIGGER trigger_fts_insert AFTER INSERT ON messages WHEN NEW.search_id IS NOT NULL BEGIN INSERT INTO messages_fts(rowid, text) VALUES(NEW.search_id, NEW.text); END;
COMMIT;
