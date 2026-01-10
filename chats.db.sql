BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "chat_folders" (
	"chat_id"	INTEGER,
	"folder_id"	INTEGER,
	PRIMARY KEY("chat_id","folder_id")
);
CREATE TABLE IF NOT EXISTS "chat_video_counts" (
	"chat_id"	INTEGER,
	"videos_count"	INTEGER DEFAULT 0,
	"scanned_at"	TEXT,
	"duplicados"	INTEGER DEFAULT 0,
	"indexados"	INTEGER DEFAULT 0,
	PRIMARY KEY("chat_id")
);
CREATE TABLE IF NOT EXISTS "chats" (
	"chat_id"	INTEGER,
	"name"	TEXT,
	"type"	TEXT,
	"photo_id"	TEXT,
	"username"	TEXT,
	"raw_json"	TEXT,
	"updated_at"	TEXT,
	"last_message_date"	TEXT,
	"ultimo_escaneo"	TEXT,
	"is_owner"	INTEGER DEFAULT 0,
	"is_public"	INTEGER DEFAULT 0,
	"has_protected_content"	INTEGER DEFAULT 0,
	"activo"	INTEGER DEFAULT 0,
	PRIMARY KEY("chat_id")
);
CREATE TABLE IF NOT EXISTS "video_file_ids" (
	"id"	INTEGER,
	"video_id"	TEXT NOT NULL,
	"file_id"	TEXT NOT NULL,
	"file_unique_id"	TEXT,
	"fecha_detectado"	TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	"origen"	TEXT DEFAULT 'scan',
	"notas"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT),
	UNIQUE("video_id","file_id"),
	FOREIGN KEY("video_id") REFERENCES "videos_telegram"("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "video_messages" (
	"id"	INTEGER,
	"video_id"	TEXT NOT NULL,
	"chat_id"	INTEGER NOT NULL,
	"message_id"	INTEGER NOT NULL,
	"date"	TEXT,
	"from_user_id"	INTEGER,
	"from_username"	TEXT,
	"from_is_bot"	INTEGER,
	"media_type"	TEXT,
	"views"	INTEGER,
	"forwards"	INTEGER,
	"outgoing"	INTEGER,
	"reply_to_message_id"	INTEGER,
	"forward_from_chat_id"	INTEGER,
	"forward_from_chat_title"	TEXT,
	"forward_from_message_id"	INTEGER,
	"forward_date"	TEXT,
	"caption"	TEXT,
	UNIQUE("chat_id","message_id"),
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "videos_telegram" (
	"id"	TEXT,
	"chat_id"	INTEGER NOT NULL,
	"message_id"	INTEGER NOT NULL,
	"file_id"	TEXT NOT NULL,
	"file_unique_id"	TEXT NOT NULL,
	"nombre"	TEXT,
	"caption"	TEXT,
	"tags_ia"	TEXT DEFAULT '[]',
	"meta_extra"	TEXT,
	"ruta_local"	TEXT,
	"ruta_mega"	TEXT,
	"tamano_bytes"	INTEGER,
	"fecha_mod"	TEXT,
	"fecha_mensaje"	TEXT,
	"fecha_descarga"	TEXT,
	"fecha_procesado"	TEXT DEFAULT '',
	"duracion"	REAL DEFAULT 0,
	"ancho"	INTEGER DEFAULT 0,
	"alto"	INTEGER DEFAULT 0,
	"es_vertical"	INTEGER DEFAULT 0,
	"codec_video"	TEXT DEFAULT '',
	"codec_audio"	TEXT DEFAULT '',
	"bitrate"	INTEGER DEFAULT 0,
	"fps"	REAL DEFAULT 0,
	"tiene_audio"	INTEGER DEFAULT 1,
	"version_sprite"	TEXT DEFAULT '',
	"es_video"	INTEGER DEFAULT 0,
	"has_sprite"	INTEGER DEFAULT 0,
	"has_thumb"	INTEGER DEFAULT 0,
	"en_mega"	INTEGER DEFAULT 1,
	"oculto"	INTEGER DEFAULT 0,
	"ffmpeg_error"	INTEGER DEFAULT 0,
	"mime_type"	TEXT,
	"views"	INTEGER DEFAULT 0,
	"outgoing"	INTEGER DEFAULT 0,
	"reply_to_message_id"	INTEGER,
	"forwarded_from"	TEXT,
	"sprite_path"	TEXT,
	"thumb_path"	TEXT,
	"download_url"	TEXT,
	"dump_message_id"	INTEGER DEFAULT NULL,
	"dump_fail"	INTEGER DEFAULT NULL,
	"thumb_bytes"	INTEGER,
	"watch_later"	INTEGER DEFAULT 0,
	UNIQUE("chat_id","message_id"),
	UNIQUE("file_unique_id"),
	PRIMARY KEY("id")
);
CREATE INDEX IF NOT EXISTS "idx_video_messages_video_id" ON "video_messages" (
	"video_id"
);
CREATE INDEX IF NOT EXISTS "idx_videos_file_unique" ON "videos_telegram" (
	"file_unique_id"
);
CREATE INDEX IF NOT EXISTS "idx_videos_oculto" ON "videos_telegram" (
	"oculto"
);
CREATE INDEX IF NOT EXISTS "idx_videos_thumb_oculto_fecha" ON "videos_telegram" (
	"has_thumb",
	"oculto",
	"fecha_mensaje"	DESC,
	"message_id"	DESC
);
CREATE INDEX IF NOT EXISTS "idx_videos_watch_later" ON "videos_telegram" (
	"watch_later"
);
COMMIT;
