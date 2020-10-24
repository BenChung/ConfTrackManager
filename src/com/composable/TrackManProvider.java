package com.composable;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.http.HTTPServerVersion;
import com.wowza.wms.http.HTTProvider2Base;
import com.wowza.wms.http.IHTTPRequest;
import com.wowza.wms.http.IHTTPResponse;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.stream.publish.IStreamActionNotify;
import com.wowza.wms.stream.publish.Playlist;
import com.wowza.wms.stream.publish.PlaylistItem;
import com.wowza.wms.stream.publish.Stream;
import com.wowza.wms.vhost.IVHost;

public class TrackManProvider extends HTTProvider2Base {

	private enum PlayState {
		HOST, QA, PREROLL, PRE_RECORD
	}
	public static class Track {
		private String trackName;
		private Playlist hostStream, qaStream, preroll, presentation;
		private String hostStreamKey, qaStreamKey;
		private Stream outputStream;
		private PlayState currentState;
		private PrerecordListener presListener = new PrerecordListener();
		
		private class PrerecordListener implements IStreamActionNotify {

			@Override
			public void onPlaylistItemStart(Stream stream, PlaylistItem playlistItem) {
				// empty, we don't care
			}

			@Override
			public void onPlaylistItemStop(Stream stream, PlaylistItem playlistItem) {
				if (Track.this.currentState == PlayState.PRE_RECORD)
					Track.this.transition(PlayState.QA); // just for now
			}
		}
		
		public Track(String trackName, Stream outputStream, Playlist hostStream, String hostStreamKey, Playlist qaStream, String qaStreamKey, Playlist preroll, Playlist presentation)
		{
			this.trackName = trackName;
			this.outputStream = outputStream;
			this.hostStream = hostStream;
			this.qaStream = qaStream;
			this.preroll = preroll;
			this.presentation = presentation;
			this.hostStreamKey = hostStreamKey;
			this.qaStreamKey = qaStreamKey;

			outputStream.addListener(presListener);
			preroll.open(outputStream);
			this.currentState = PlayState.PREROLL;
		}
		
		public void transition(PlayState new_state) {
			switch (new_state) {
			case HOST:
				hostStream.open(outputStream); break;
			case QA:
				qaStream.open(outputStream); break;
			case PREROLL:
				preroll.open(outputStream); break;
			case PRE_RECORD:
				presentation.open(outputStream);
				break;
			}
		}
		
		public void setPrerecord(String asset) {
			presentation = new Playlist("prerec" + trackName);
			presentation.addItem(asset, 0, -1);
			if (currentState == PlayState.PRE_RECORD) {
				presentation.open(outputStream);
			}
		}
		
		public void setPreroll(String asset) {
			preroll = new Playlist("preroll" + trackName);
			preroll.addItem(asset, 0, -1);
			preroll.setRepeat(true);
			if (currentState == PlayState.PREROLL) {
				preroll.open(outputStream);
			}
		}
		
		public String toString() {
			return "Track " + trackName + " Host key: " + hostStreamKey + " QA key: " + qaStreamKey + " current state: " + currentState;
		}

		public void dispose() {
			outputStream.close();
		}
	}

	private static int numTracksMade = 0;
	private HashMap<String, Track> tracks = new HashMap<String, Track>();
	private static final Class<TrackManProvider> CLASS = TrackManProvider.class;
	private WMSLogger logger = WMSLoggerFactory.getLogger(CLASS);
	
	@Override
	public void onHTTPRequest(IVHost vhost, IHTTPRequest req, IHTTPResponse resp) {
		if (!doHTTPAuthentication(vhost, req, resp))
			return;
		Set<String> paramNames = req.getParameterNames();
		StringBuffer ret = new StringBuffer();
		IApplicationInstance appInstance = null;
		if (paramNames.contains("appName"))
		{
			String appName = req.getParameter("appName");
			String appInstanceName = req.getParameter("appInstanceName");
			appInstanceName = appInstanceName == null ? "_definst_" : appInstanceName;
			try
			{
				appInstance = vhost.getApplication(appName).getAppInstance(appInstanceName);
			}
			catch (Exception e)
			{
				logger.error("Invalid appInstance: " + e);
			}
			if (appInstance != null)
				handleRequest(appInstance, paramNames, req, ret);
			else 
				ret.append("Requires a valid application name.");
		} else {
			ret.append("Requires a valid application name.");
		}
		
		try
		{
			resp.setHeader("Content-Type", "text/plain");

			OutputStream out = resp.getOutputStream();
			byte[] outBytes = ret.toString().getBytes();
			out.write(outBytes);
		}
		catch (Exception e)
		{ 
			WMSLoggerFactory.getLogger(HTTPServerVersion.class).error("HTTPProviderStreamReset.onHTTPRequest: "+e.toString());
			e.printStackTrace();
		}
	}
	
	private void handleRequest(IApplicationInstance appInstance, Set<String> paramNames, IHTTPRequest req, StringBuffer ret) {
		String action = null, trackName = null, asset = null, state = null;
		if (paramNames.contains("action"))
			action = req.getParameter("action");
		if (paramNames.contains("trackName"))
			trackName = req.getParameter("trackName");
		if (paramNames.contains("asset"))
			asset = req.getParameter("asset");
		if (paramNames.contains("state"))
			state = req.getParameter("state");
		if (action == null) {
			ret.append("No action specified; must be create, setpreroll, setprerecord, transition, delete, or get.");
			return;
		}
		try {
			switch (action.toLowerCase()) {
			case "create": createTrack(appInstance, trackName); break;
			case "setpreroll": setTrackPreroll(trackName, asset); break;
			case "setprerecord": setTrackPrerecord(trackName, asset); break;
			case "transition": trackTransition(trackName, state); break;
			case "delete": deleteTrack(trackName); break;
			case "get": getTracks(ret); break;
			default: ret.append("No valid action provided; must be create, setpreroll, setprerecord, transition, delete, or get");
			}
		} catch (RuntimeException e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			ret.append(sw.toString());
		}
	}
	
	private String makeExternalStreamKey() {
		return UUID.randomUUID().toString();
	}
	
	private void createTrack(IApplicationInstance appInstance, String trackName) {
		if (trackName == null)
			throw new RuntimeException("A track name must be provided.");
		if (tracks.containsKey(trackName)) {
			throw new RuntimeException("Track with name " + trackName + " already exists");
		}
		
		Stream outputStream = Stream.createInstance(appInstance, "track_stream" + numTracksMade);
		Playlist hostStream = new Playlist("Hs" + numTracksMade);
		Playlist qaStream = new Playlist("Qas" + numTracksMade);
		Playlist preroll = new Playlist("pr" + numTracksMade);
		Playlist presentation = new Playlist("pres" + numTracksMade);
		numTracksMade += 1;
		
		String hostStreamKey = makeExternalStreamKey();
		String qaStreamKey = makeExternalStreamKey();
		hostStream.addItem(hostStreamKey, -2, -1);
		qaStream.addItem(qaStreamKey, -2, -1);
		preroll.setRepeat(true);
		
		tracks.put(trackName, new Track(trackName, outputStream, hostStream, hostStreamKey, qaStream, qaStreamKey, preroll, presentation));
	}
	
	private void setTrackPreroll(String trackName, String asset) {
		if (trackName == null)
			throw new RuntimeException("A track name must be provided.");
		if (!tracks.containsKey(trackName))
			throw new RuntimeException("No track with name " + trackName + "exists");
		if (asset == null)
			throw new RuntimeException("A asset name must be provided.");
		tracks.get(trackName).setPreroll(asset);
	}
	
	private void setTrackPrerecord(String trackName, String asset) {
		if (trackName == null)
			throw new RuntimeException("A track name must be provided.");
		if (!tracks.containsKey(trackName))
			throw new RuntimeException("No track with name " + trackName + "exists");
		if (asset == null)
			throw new RuntimeException("A asset name must be provided.");
		tracks.get(trackName).setPrerecord(asset);
	}

	private void trackTransition(String trackName, String newState) {
		if (trackName == null)
			throw new RuntimeException("A track name must be provided.");
		if (!tracks.containsKey(trackName))
			throw new RuntimeException("No track with name " + trackName + "exists");
		if (newState == null)
			throw new RuntimeException("A state must be provided.");
		Track track = tracks.get(trackName);
		switch (newState.toLowerCase()) {
		case "host": track.transition(PlayState.HOST); break;
		case "qa": track.transition(PlayState.QA); break;
		case "preroll": track.transition(PlayState.PREROLL); break;
		case "prerecord": track.transition(PlayState.PRE_RECORD); break;
		default: throw new RuntimeException("State name " + newState + " invalid; valid options are 'host', 'qa', 'preroll', and 'prerecord'");
		}
	}
	
	private void deleteTrack(String trackName) {
		if (trackName == null)
			throw new RuntimeException("A track name must be provided.");
		if (!tracks.containsKey(trackName))
			throw new RuntimeException("No track with name " + trackName + "exists");
		tracks.get(trackName).dispose();
		tracks.remove(trackName);
	}
	
	private void getTracks(StringBuffer output) {
		output.append(String.join("\n", tracks.values().stream().map(x->x.toString()).collect(Collectors.toList())));
	}
}
