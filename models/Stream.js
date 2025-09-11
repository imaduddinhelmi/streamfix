const { v4: uuidv4 } = require('uuid');
const { db } = require('../db/database');
class Stream {
  static create(streamData) {
    const id = uuidv4();
    const {
      title,
      video_id,
      rtmp_url,
      stream_key,
      platform,
      platform_icon,
      bitrate = 2500,
      resolution,
      fps = 30,
      orientation = 'horizontal',
      loop_video = true,
      schedule_time = null,
      duration = null,
      use_advanced_settings = false,
      is_daily_schedule = false,
      daily_start_time = null,
      daily_end_time = null,
      daily_days = null,
      user_id
    } = streamData;
    const loop_video_int = loop_video ? 1 : 0;
    const use_advanced_settings_int = use_advanced_settings ? 1 : 0;
    const is_daily_schedule_int = is_daily_schedule ? 1 : 0;
    const status = schedule_time ? 'scheduled' : (is_daily_schedule ? 'daily_scheduled' : 'offline');
    const status_updated_at = new Date().toISOString();
    
    return new Promise((resolve, reject) => {
      // Build dynamic INSERT statement with all required columns
      const fields = [
        'id', 'title', 'video_id', 'rtmp_url', 'stream_key', 'platform', 'platform_icon',
        'bitrate', 'resolution', 'fps', 'orientation', 'loop_video',
        'schedule_time', 'duration', 'status', 'status_updated_at', 'start_time', 'end_time',
        'use_advanced_settings', 'created_at', 'updated_at', 'user_id',
        'is_daily_schedule', 'daily_start_time', 'daily_end_time', 'daily_days', 'last_daily_run'
      ];
      
      const now = new Date().toISOString();
      const values = [
        id, title, video_id, rtmp_url, stream_key, platform, platform_icon,
        bitrate, resolution, fps, orientation, loop_video_int,
        schedule_time, duration, status, status_updated_at, null, null,
        use_advanced_settings_int, now, now, user_id,
        is_daily_schedule_int, daily_start_time, daily_end_time, daily_days, null
      ];
      
      const placeholders = values.map(() => '?').join(', ');
      const fieldNames = fields.join(', ');
      
      const query = `INSERT INTO streams (${fieldNames}) VALUES (${placeholders})`;
      
      db.run(query, values, function (err) {
        if (err) {
          console.error('Error creating stream:', err.message);
          return reject(err);
        }
        resolve({ id, ...streamData, status, status_updated_at });
      });
    });
  }
  static findById(id) {
    return new Promise((resolve, reject) => {
      db.get('SELECT * FROM streams WHERE id = ?', [id], (err, row) => {
        if (err) {
          console.error('Error finding stream:', err.message);
          return reject(err);
        }
        if (row) {
          row.loop_video = row.loop_video === 1;
          row.use_advanced_settings = row.use_advanced_settings === 1;
          row.is_daily_schedule = row.is_daily_schedule === 1;
        }
        resolve(row);
      });
    });
  }
  static findAll(userId = null, filter = null) {
    return new Promise((resolve, reject) => {
      let query = `
        SELECT s.*, 
               v.title AS video_title, 
               v.filepath AS video_filepath,
               v.thumbnail_path AS video_thumbnail, 
               v.duration AS video_duration,
               v.resolution AS video_resolution,  
               v.bitrate AS video_bitrate,        
               v.fps AS video_fps                 
        FROM streams s
        LEFT JOIN videos v ON s.video_id = v.id
      `;
      const params = [];
      if (userId) {
        query += ' WHERE s.user_id = ?';
        params.push(userId);
        if (filter) {
          if (filter === 'live') {
            query += " AND s.status = 'live'";
          } else if (filter === 'scheduled') {
            query += " AND s.status = 'scheduled'";
          } else if (filter === 'offline') {
            query += " AND s.status = 'offline'";
          }
        }
      }
      query += ' ORDER BY s.created_at DESC';
      db.all(query, params, (err, rows) => {
        if (err) {
          console.error('Error finding streams:', err.message);
          return reject(err);
        }
        if (rows) {
          rows.forEach(row => {
            row.loop_video = row.loop_video === 1;
            row.use_advanced_settings = row.use_advanced_settings === 1;
            row.is_daily_schedule = row.is_daily_schedule === 1;
          });
        }
        resolve(rows || []);
      });
    });
  }
  static update(id, streamData) {
    const fields = [];
    const values = [];
    Object.entries(streamData).forEach(([key, value]) => {
      if ((key === 'loop_video' || key === 'use_advanced_settings' || key === 'is_daily_schedule') && typeof value === 'boolean') {
        fields.push(`${key} = ?`);
        values.push(value ? 1 : 0);
      } else {
        fields.push(`${key} = ?`);
        values.push(value);
      }
    });
    fields.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);
    const query = `UPDATE streams SET ${fields.join(', ')} WHERE id = ?`;
    return new Promise((resolve, reject) => {
      db.run(query, values, function (err) {
        if (err) {
          console.error('Error updating stream:', err.message);
          return reject(err);
        }
        resolve({ id, ...streamData });
      });
    });
  }
  static delete(id, userId) {
    return new Promise((resolve, reject) => {
      db.run(
        'DELETE FROM streams WHERE id = ? AND user_id = ?',
        [id, userId],
        function (err) {
          if (err) {
            console.error('Error deleting stream:', err.message);
            return reject(err);
          }
          resolve({ success: true, deleted: this.changes > 0 });
        }
      );
    });
  }
  static updateStatus(id, status, userId) {
    const status_updated_at = new Date().toISOString();
    let start_time = null;
    let end_time = null;
    if (status === 'live') {
      start_time = new Date().toISOString();
    } else if (status === 'offline') {
      end_time = new Date().toISOString();
    }
    return new Promise((resolve, reject) => {
      db.run(
        `UPDATE streams SET 
          status = ?, 
          status_updated_at = ?, 
          start_time = COALESCE(?, start_time), 
          end_time = COALESCE(?, end_time),
          updated_at = CURRENT_TIMESTAMP
         WHERE id = ? AND user_id = ?`,
        [status, status_updated_at, start_time, end_time, id, userId],
        function (err) {
          if (err) {
            console.error('Error updating stream status:', err.message);
            return reject(err);
          }
          resolve({
            id,
            status,
            status_updated_at,
            start_time,
            end_time,
            updated: this.changes > 0
          });
        }
      );
    });
  }
  static async getStreamWithVideo(id) {
    return new Promise((resolve, reject) => {
      db.get(
        `SELECT s.*, v.title AS video_title, v.filepath AS video_filepath, 
                v.thumbnail_path AS video_thumbnail, v.duration AS video_duration
         FROM streams s
         LEFT JOIN videos v ON s.video_id = v.id
         WHERE s.id = ?`,
        [id],
        (err, row) => {
          if (err) {
            console.error('Error fetching stream with video:', err.message);
            return reject(err);
          }
          if (row) {
            row.loop_video = row.loop_video === 1;
            row.use_advanced_settings = row.use_advanced_settings === 1;
            row.is_daily_schedule = row.is_daily_schedule === 1;
          }
          resolve(row);
        }
      );
    });
  }
  static async isStreamKeyInUse(streamKey, userId, excludeId = null) {
    return new Promise((resolve, reject) => {
      let query = 'SELECT COUNT(*) as count FROM streams WHERE stream_key = ? AND user_id = ?';
      const params = [streamKey, userId];
      if (excludeId) {
        query += ' AND id != ?';
        params.push(excludeId);
      }
      db.get(query, params, (err, row) => {
        if (err) {
          console.error('Error checking stream key:', err.message);
          return reject(err);
        }
        resolve(row.count > 0);
      });
    });
  }
  static findScheduledInRange(startTime, endTime) {
    return new Promise((resolve, reject) => {
      const startTimeStr = startTime.toISOString();
      const endTimeStr = endTime.toISOString();
      const query = `
        SELECT s.*, 
               v.title AS video_title, 
               v.filepath AS video_filepath,
               v.thumbnail_path AS video_thumbnail, 
               v.duration AS video_duration,
               v.resolution AS video_resolution,
               v.bitrate AS video_bitrate,
               v.fps AS video_fps  
        FROM streams s
        LEFT JOIN videos v ON s.video_id = v.id
        WHERE s.status = 'scheduled'
        AND s.schedule_time IS NOT NULL
        AND s.schedule_time >= ?
        AND s.schedule_time <= ?
      `;
      db.all(query, [startTimeStr, endTimeStr], (err, rows) => {
        if (err) {
          console.error('Error finding scheduled streams:', err.message);
          return reject(err);
        }
        if (rows) {
          rows.forEach(row => {
            row.loop_video = row.loop_video === 1;
            row.use_advanced_settings = row.use_advanced_settings === 1;
            row.is_daily_schedule = row.is_daily_schedule === 1;
          });
        }
        resolve(rows || []);
      });
    });
  }

  static findDailyScheduledStreams() {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT s.*, 
               v.title AS video_title, 
               v.filepath AS video_filepath,
               v.thumbnail_path AS video_thumbnail, 
               v.duration AS video_duration,
               v.resolution AS video_resolution,
               v.bitrate AS video_bitrate,
               v.fps AS video_fps  
        FROM streams s
        LEFT JOIN videos v ON s.video_id = v.id
        WHERE s.is_daily_schedule = 1
        AND s.status = 'daily_scheduled'
        AND s.daily_start_time IS NOT NULL
        AND s.daily_days IS NOT NULL
      `;
      db.all(query, [], (err, rows) => {
        if (err) {
          console.error('Error finding daily scheduled streams:', err.message);
          return reject(err);
        }
        if (rows) {
          rows.forEach(row => {
            row.loop_video = row.loop_video === 1;
            row.use_advanced_settings = row.use_advanced_settings === 1;
            row.is_daily_schedule = row.is_daily_schedule === 1;
          });
        }
        resolve(rows || []);
      });
    });
  }

  static updateLastDailyRun(streamId) {
    return new Promise((resolve, reject) => {
      db.run(
        'UPDATE streams SET last_daily_run = CURRENT_TIMESTAMP WHERE id = ?',
        [streamId],
        function (err) {
          if (err) {
            console.error('Error updating last daily run:', err.message);
            return reject(err);
          }
          resolve({ success: true, changes: this.changes });
        }
      );
    });
  }
}
module.exports = Stream;