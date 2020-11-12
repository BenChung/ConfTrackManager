/*
 * This code and all components (c) Copyright 2006 - 2018, Wowza Media Systems, LLC. All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */
package com.composable;

import java.util.Map;

import com.composable.ServerListenerStreamPublisher.StreamRunner;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.client.*;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.logging.WMSLoggerIDs;
import com.wowza.wms.mediacaster.IMediaCaster;
import com.wowza.wms.mediacaster.IMediaCasterNotify2;
import com.wowza.wms.module.*;
import com.wowza.wms.request.*;
import com.wowza.wms.server.Server;
import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.stream.IMediaStreamActionNotify;
import com.wowza.wms.stream.IMediaStreamPlay;
import com.wowza.wms.stream.publish.Stream;

public class ModuleStreamPublisher extends ModuleBase
{
	public static final String MODULE_NAME = "ModuleStreamPublisher";
	public static final String PROP_NAME_PREFIX = "streamPublisher";
	
	private ServerListenerStreamPublisher streamPublisher;
	private IApplicationInstance appInstance;
	private WMSLogger logger;
	
	/**
	 * 
	 * Flash client loadSchedule method.
	 */
	public void loadSchedule(IClient client, RequestFunction function, AMFDataList params)
	{
		try
		{
			sendResult(client, params, loadSchedule());
		}
		catch (Exception e)
		{
			sendResult(client, params, e.getMessage());
		}
	}
	
	private class StreamListener implements IMediaStreamActionNotify
	{
		public void onPublish(IMediaStream stream, final String streamName, boolean isRecord, boolean isAppend)
		{
			logger.info(MODULE_NAME +".onPublish "+streamName);
			if (streamPublisher != null) {
				streamPublisher.notifyStreamPublish(appInstance, streamName);
			}

		}

		public void onUnPublish(IMediaStream stream, String streamName, boolean isRecord, boolean isAppend)
		{
			logger.info(MODULE_NAME +".onUnPublish "+streamName);
			if (streamPublisher != null) {
				streamPublisher.notifyStreamUnpublish(appInstance, streamName);
			}
		}

		public void onPause(IMediaStream stream, boolean isPause, double location)
		{
		}

		public void onPlay(IMediaStream stream, String streamName, double playStart, double playLen, int playReset)
		{
		}

		public void onSeek(IMediaStream stream, double location)
		{
		}

		public void onStop(IMediaStream stream)
		{
		}
	}

	StreamListener streamListener = new StreamListener();
	public void onStreamCreate(IMediaStream stream)
	{
		logger.info(MODULE_NAME +".onStreamCreate "+stream.getName());
		stream.addClientListener(streamListener);
	}

	public void onStreamDestroy(IMediaStream stream)
	{
		logger.info(MODULE_NAME +".onStreamDestroy "+stream.getName());
		stream.removeClientListener(streamListener);
	}
	/**
	 * 
	 * Get the StreamPublisher and save it as a Server property if it doesn't already exist then load schedule.
	 */
	public void onAppStart(IApplicationInstance appInstance)
	{
		this.appInstance = appInstance;
		this.logger = WMSLoggerFactory.getLoggerObj(appInstance);
		logger.info(MODULE_NAME + ".onAppStart: ["+appInstance.getContextStr()+"]: Build #7", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
		
		streamPublisher = (ServerListenerStreamPublisher)Server.getInstance().getProperties().get(ServerListenerStreamPublisher.PROP_STREAMPUBLISHER);
		if(streamPublisher == null)
		{
			streamPublisher = new ServerListenerStreamPublisher();
			Server.getInstance().getProperties().setProperty(ServerListenerStreamPublisher.PROP_STREAMPUBLISHER, streamPublisher);
		}
		appInstance.getVHost().getThreadPool().execute(new Runnable() {

			@Override
			public void run()
			{
				try
				{
					String ret = loadSchedule();			
					//logger.info(MODULE_NAME + ".onAppStart: ["+appInstance.getContextStr()+"]: "+ret, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				}
				catch (Exception e)
				{
					logger.error("ModuleStreamPublisher.onAppStart: ["+appInstance.getContextStr()+"]: " + e.getMessage(), e);
				}
			}
			
		});
	}

	/**
	 * 
	 * Stop streams and remove local reference to streamPublisher.
	 */
	public void onAppStop(IApplicationInstance appInstance)
	{
		unloadSchedule();
		streamPublisher = null;
	}
	
	/**
	 * public loadSchedule method.  Can be called from JMX.
	 * @throws Exception 
	 */
	public String loadSchedule() throws Exception
	{
		return streamPublisher.startupModule(appInstance);
	}
	
	/**
	 * public unloadSchedule method.  Can be called from JMX.
	 */
	@SuppressWarnings("unchecked")
	public void unloadSchedule()
	{
		Map<String, StreamRunner> streams = (Map<String, StreamRunner>)appInstance.getProperties().remove(PROP_NAME_PREFIX + "Runners");
		if(streams != null)
		{
			for(StreamRunner stream : streams.values())
			{
				stream.shutdown(appInstance);
			}
			streams.clear();
		}
		appInstance.getProperties().remove(PROP_NAME_PREFIX + "ScheduleLoaded");
	}

}
