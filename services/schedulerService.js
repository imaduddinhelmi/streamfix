const Stream = require('../models/Stream');
const scheduledTerminations = new Map();
const SCHEDULE_LOOKAHEAD_SECONDS = 60;
let streamingService = null;
function init(streamingServiceInstance) {
  streamingService = streamingServiceInstance;
  console.log('Stream scheduler initialized');
  setInterval(checkScheduledStreams, 60 * 1000);
  setInterval(checkStreamDurations, 60 * 1000);
  checkScheduledStreams();
  checkStreamDurations();
}
async function checkScheduledStreams() {
  try {
    if (!streamingService) {
      console.error('StreamingService not initialized in scheduler');
      return;
    }
    const now = new Date();
    const lookAheadTime = new Date(now.getTime() + SCHEDULE_LOOKAHEAD_SECONDS * 1000);
    console.log(`Checking for scheduled streams (${now.toISOString()} to ${lookAheadTime.toISOString()})`);
    
    // Check one-time scheduled streams
    const streams = await Stream.findScheduledInRange(now, lookAheadTime);
    if (streams.length > 0) {
      console.log(`Found ${streams.length} one-time scheduled streams to start`);
      for (const stream of streams) {
        console.log(`Starting scheduled stream: ${stream.id} - ${stream.title}`);
        const result = await streamingService.startStream(stream.id);
        if (result.success) {
          console.log(`Successfully started scheduled stream: ${stream.id}`);
          if (stream.duration) {
            scheduleStreamTermination(stream.id, stream.duration);
          }
        } else {
          console.error(`Failed to start scheduled stream ${stream.id}: ${result.error}`);
        }
      }
    }

    // Check daily scheduled streams
    await checkDailyScheduledStreams();
  } catch (error) {
    console.error('Error checking scheduled streams:', error);
  }
}

async function checkDailyScheduledStreams() {
  try {
    const now = new Date();
    const currentDay = now.getDay(); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    const currentTime = now.toTimeString().substring(0, 5); // HH:MM format
    
    console.log(`Checking daily scheduled streams (Day: ${currentDay}, Time: ${currentTime})`);
    
    const dailyStreams = await Stream.findDailyScheduledStreams();
    
    for (const stream of dailyStreams) {
      try {
        // Parse the days string (e.g., "1,2,3,4,5" for weekdays)
        const scheduledDays = stream.daily_days.split(',').map(day => parseInt(day.trim()));
        
        // Check if today is a scheduled day
        if (!scheduledDays.includes(currentDay)) {
          continue;
        }
        
        // Check if it's time to start the stream
        if (currentTime < stream.daily_start_time) {
          continue;
        }
        
        // Check if we already ran this stream today
        if (stream.last_daily_run) {
          const lastRun = new Date(stream.last_daily_run);
          const today = new Date();
          if (lastRun.toDateString() === today.toDateString()) {
            console.log(`Daily stream ${stream.id} already ran today, skipping`);
            continue;
          }
        }
        
        console.log(`Starting daily scheduled stream: ${stream.id} - ${stream.title}`);
        const result = await streamingService.startStream(stream.id);
        
        if (result.success) {
          console.log(`Successfully started daily stream: ${stream.id}`);
          
          // Update last daily run timestamp
          await Stream.updateLastDailyRun(stream.id);
          
          // Schedule termination if there's an end time
          if (stream.daily_end_time) {
            const startTime = new Date();
            const [endHour, endMinute] = stream.daily_end_time.split(':').map(Number);
            const endTime = new Date();
            endTime.setHours(endHour, endMinute, 0, 0);
            
            // If end time is tomorrow, add 24 hours
            if (endTime <= startTime) {
              endTime.setDate(endTime.getDate() + 1);
            }
            
            const durationMinutes = (endTime - startTime) / (1000 * 60);
            scheduleStreamTermination(stream.id, durationMinutes);
            console.log(`Scheduled daily stream ${stream.id} to end at ${stream.daily_end_time}`);
          } else if (stream.duration) {
            scheduleStreamTermination(stream.id, stream.duration);
          }
        } else {
          console.error(`Failed to start daily stream ${stream.id}: ${result.error}`);
        }
      } catch (error) {
        console.error(`Error processing daily stream ${stream.id}:`, error);
      }
    }
  } catch (error) {
    console.error('Error checking daily scheduled streams:', error);
  }
}
async function checkStreamDurations() {
  try {
    if (!streamingService) {
      console.error('StreamingService not initialized in scheduler');
      return;
    }
    const liveStreams = await Stream.findAll(null, 'live');
    for (const stream of liveStreams) {
      if (stream.duration && stream.start_time && !scheduledTerminations.has(stream.id)) {
        const startTime = new Date(stream.start_time);
        const durationMs = stream.duration * 60 * 1000;
        const shouldEndAt = new Date(startTime.getTime() + durationMs);
        const now = new Date();
        if (shouldEndAt <= now) {
          console.log(`Stream ${stream.id} exceeded duration, stopping now`);
          await streamingService.stopStream(stream.id);
        } else {
          const timeUntilEnd = shouldEndAt.getTime() - now.getTime();
          scheduleStreamTermination(stream.id, timeUntilEnd / 60000);
        }
      }
    }
  } catch (error) {
    console.error('Error checking stream durations:', error);
  }
}
function scheduleStreamTermination(streamId, durationMinutes) {
  if (scheduledTerminations.has(streamId)) {
    clearTimeout(scheduledTerminations.get(streamId));
  }
  const durationMs = durationMinutes * 60 * 1000;
  console.log(`Scheduling termination for stream ${streamId} after ${durationMinutes} minutes`);
  const timeoutId = setTimeout(async () => {
    try {
      console.log(`Terminating stream ${streamId} after ${durationMinutes} minute duration`);
      await streamingService.stopStream(streamId);
      scheduledTerminations.delete(streamId);
    } catch (error) {
      console.error(`Error terminating stream ${streamId}:`, error);
    }
  }, durationMs);
  scheduledTerminations.set(streamId, timeoutId);
}
function cancelStreamTermination(streamId) {
  if (scheduledTerminations.has(streamId)) {
    clearTimeout(scheduledTerminations.get(streamId));
    scheduledTerminations.delete(streamId);
    console.log(`Cancelled scheduled termination for stream ${streamId}`);
    return true;
  }
  return false;
}
function handleStreamStopped(streamId) {
  return cancelStreamTermination(streamId);
}
module.exports = {
  init,
  scheduleStreamTermination,
  cancelStreamTermination,
  handleStreamStopped
};