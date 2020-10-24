package com.composable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.wowza.wms.stream.publish.Playlist;
import com.wowza.wms.stream.publish.Stream;
import com.wowza.wms.timedtext.model.TimedTextEntry;

public class VideoScheduler {
	private final class PlaylistComparator implements Comparator<ScheduledPlaylist> {
		@Override
		public int compare(ScheduledPlaylist o1, ScheduledPlaylist o2) {
			return Long.compare(o1.scheduledTime, o2.scheduledTime);
		}
	}
	private class ScheduledPlaylist {
		public long scheduledTime;
		public String stream;
		public boolean repeat;
		public String id;
		public ArrayList<ScheduledVideo> videos;
		public ScheduledPlaylist(long scheduledTime, String stream, boolean repeat, String id, ArrayList<ScheduledVideo> videos) {
			this.scheduledTime = scheduledTime;
			this.stream = stream;
			this.repeat = repeat;
			this.id = id;
			this.videos = videos;
		}
		
		private PlaylistRunner runner = null;
		private boolean scheduled = true; // has this playlist already ran?
		public void schedule(Timer into, Stream on) {
			if (runner == null)
				runner = new PlaylistRunner(this, on);
			into.schedule(runner, scheduledTime);
		}
		
		private Playlist makePlaylist() {
			Playlist outpPlaylist = new Playlist(id);
			for (ScheduledVideo video : videos)
				outpPlaylist.addItem(video.id, video.start, video.length);
			outpPlaylist.setRepeat(repeat);
			return outpPlaylist;
		}
		
		// update this playlist to match the given playlist
		public void update(ScheduledPlaylist scheduledPlaylist, boolean soft) {
			if (this.scheduledTime != scheduledPlaylist.scheduledTime && 
					(!soft || scheduledPlaylist.scheduledTime < new Date().getTime()) && 
					scheduled && runner != null)
				runner.reschedule(scheduledPlaylist.scheduledTime);
			this.scheduledTime = scheduledPlaylist.scheduledTime;
			this.stream = scheduledPlaylist.stream;
			this.repeat = scheduledPlaylist.repeat;
			this.id = scheduledPlaylist.id;
			
		}

		public void unschedule() {
			runner.cancel();
		}
		public void ran() {
			this.scheduled = false; // the event has happened
		}
	}
	private class ScheduledVideo {
		public int length;
		public int start;
		public String id;
		public String src;
		public List<TimedTextEntry> captions;
	}
	
	private class PlaylistRunner extends TimerTask {
		
		private ScheduledPlaylist playlist;
		private Stream target;
		public PlaylistRunner(ScheduledPlaylist myPlaylist, Stream target) {
			this.playlist = myPlaylist;
			this.target = target;
		}
		
		// reschedule this runner to run at a different time
		public void reschedule(long newTime) {
			this.cancel();
			timer.schedule(this, newTime);
		}

		@Override
		public void run() {
			playlist.ran();
			playlist.makePlaylist().open(target);
		}
		
	}

	private Timer timer = new Timer();
	
	private HashMap<String, ScheduledPlaylist> scheduled = new HashMap<>();
	private HashMap<String, List<ScheduledPlaylist>> streamSchedules = new HashMap<>();
	private HashMap<String, Stream> streams = new HashMap<>();
	
	public void schedule(Set<String> newStreams, HashMap<String, ScheduledPlaylist> newSchedule, boolean soft) {
		// clear the streams that aren't in the new schedule
		HashSet<String> streamsToRemove = new HashSet<>(streams.keySet());
		streamsToRemove.removeAll(newStreams);
		
		
		HashSet<String> streamsToAdd = new HashSet<>(newSchedule.keySet());
		streamsToAdd.removeAll(streams.keySet());
		
		// there are three sets that we need to worry about
		// 1: the events in scheduled that are NOT in newSchedule; these should be cancelled
		// 2: the events in scheduled that ARE in newSchedule; these should be updated
		// 3: the events in newSchedule that are not in newSchedule; these should be created
		// additionally, we need to deal with playlists that are currently playing
		//   if soft: don't touch them, just schedule into the future
		//   if hard: schedule the "last" playlist that will then start immediately
	
		// compute the set of scheduled events that should be removed.
		HashSet<String> toRemove = new HashSet<>(scheduled.keySet()); // get all of the existing scheduled events
		toRemove.removeAll(newSchedule.keySet()); // remove those that are in the new schedule
		
		// compute the set of scheduled events that should be updated
		HashSet<String> toUpdate = new HashSet<>(scheduled.keySet());
		toUpdate.retainAll(newSchedule.keySet());
		
		// compute the set of new events
		HashSet<String> toAdd = new HashSet<>(newSchedule.keySet());
		toAdd.removeAll(scheduled.keySet());
		
		// we first remove all events that do not exist in the new schedule
		for (String removeKey : toRemove) {
			scheduled.get(removeKey).unschedule();
			scheduled.remove(removeKey);
		}
		
		// we then update all of the remaining events
		for (String updateKey : toUpdate) {
			scheduled.get(updateKey).update(newSchedule.get(updateKey), soft);
		}
		
		// then add the new events
		for (String newKey : toAdd) {
			ScheduledPlaylist newPlaylist = newSchedule.get(newKey);
			scheduled.put(newKey, newPlaylist);
		}
		
		// rebuild the stream schedules
		rebuildStreamSchedules();
		
		// start the new events

		for (String newKey : toAdd) {
			//newSchedule.get(newKey).schedule(timer, on);
		}
	}
	
	private void rebuildStreamSchedules() {
		streamSchedules.clear();
		//build the new schedules
		for (ScheduledPlaylist playlist : scheduled.values()) {
			if (!streamSchedules.containsKey(playlist.stream)) {
				streamSchedules.put(playlist.stream, new ArrayList<ScheduledPlaylist>());
			}
			streamSchedules.get(playlist.stream).add(playlist);
		}
		
		//sort the events in each stream
		PlaylistComparator comparator = new PlaylistComparator();
		for (String stream : streamSchedules.keySet()) {
			streamSchedules.get(stream).sort(comparator);
		}
	}
	

}
