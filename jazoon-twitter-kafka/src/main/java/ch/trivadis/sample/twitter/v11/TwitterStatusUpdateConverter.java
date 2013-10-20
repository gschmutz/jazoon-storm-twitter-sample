package ch.trivadis.sample.twitter.v11;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;
import ch.trivadis.sample.twitter.avro.v11.TwitterGeoLocation;
import ch.trivadis.sample.twitter.avro.v11.TwitterHashtagEntity;
import ch.trivadis.sample.twitter.avro.v11.TwitterMediaEntity;
import ch.trivadis.sample.twitter.avro.v11.TwitterPlace;
import ch.trivadis.sample.twitter.avro.v11.TwitterStatusUpdate;
import ch.trivadis.sample.twitter.avro.v11.TwitterURLEntity;
import ch.trivadis.sample.twitter.avro.v11.TwitterUser;
import ch.trivadis.sample.twitter.avro.v11.TwitterUserMentionEntity;
import ch.trivadis.sample.util.DateUtil;

public class TwitterStatusUpdateConverter {

	private static Logger logger = Logger
			.getLogger(TwitterStatusUpdateConverter.class);

	private static TwitterStatusUpdate initializeTwitterStatusUpdate() {
		TwitterStatusUpdate TwitterStatusUpdate = new TwitterStatusUpdate();
		TwitterStatusUpdate.setUser(new TwitterUser());

		TwitterPlace place = new TwitterPlace();
		place.setBoundingBox(new ArrayList<TwitterGeoLocation>());
		TwitterStatusUpdate.setPlace(place);

		return TwitterStatusUpdate;
	}

	public static TwitterStatusUpdate convert(Status status) {
		TwitterStatusUpdate TwitterStatusUpdate = initializeTwitterStatusUpdate();
		try {
			/* Setting contributors in TwitterStatusUpdate */
			ArrayList<Long> contributors = new ArrayList<Long>();
			for (long contributor : status.getContributors())
				contributors.add(contributor);
			TwitterStatusUpdate.setContributors(contributors);

			/* Setting basic elements in TwitterStatusUpdate */
			TwitterStatusUpdate.setCreatedAt(DateUtil.getDateAsString(status.getCreatedAt()));
			TwitterStatusUpdate.setCreatedAtAsLong(status.getCreatedAt().getTime());
			//TwitterStatusUpdate.setCurrentUserRetweetId(status
			//		.getCurrentUserRetweetId());
			TwitterStatusUpdate.setId(status.getId());
			TwitterStatusUpdate.setInReplyToScreenName(status
					.getInReplyToScreenName());
			TwitterStatusUpdate.setInReplyToStatusId(status
					.getInReplyToStatusId());
			TwitterStatusUpdate.setInReplyToUserId(status.getInReplyToUserId());
			TwitterStatusUpdate.setRetweetCount(status.getRetweetCount());
			TwitterStatusUpdate.setSource(status.getSource());
			TwitterStatusUpdate.setText(status.getText());
			TwitterStatusUpdate.setIsFavorited(status.isFavorited());
			//TwitterStatusUpdate.setIsPossiblySensitive(status
			//		.isPossiblySensitive());
			TwitterStatusUpdate.setIsRetweet(status.isRetweet());
			TwitterStatusUpdate.setIsRetweetedByMe(status.isRetweetedByMe());
			TwitterStatusUpdate.setIsTruncated(status.isTruncated());

			/* Setting basic GeoLocation elements */
			if (status.getGeoLocation() != null) {
				TwitterStatusUpdate.setCoordinatesLatitude(status
						.getGeoLocation().getLatitude());
				TwitterStatusUpdate.setCoordinatesLongitude(status
						.getGeoLocation().getLongitude());
			}

			/* Setting TwitterHashtagEntities */
			ArrayList<TwitterHashtagEntity> hashtagEntities = new ArrayList<TwitterHashtagEntity>();
			for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
				TwitterHashtagEntity simpleHashtagEntity = new TwitterHashtagEntity();
				simpleHashtagEntity.setEnd(hashtagEntity.getEnd());
				simpleHashtagEntity.setStart(hashtagEntity.getStart());
				simpleHashtagEntity.setText(hashtagEntity.getText());
				hashtagEntities.add(simpleHashtagEntity);
			}
			TwitterStatusUpdate.setHashtagEntities(hashtagEntities);

			/* Setting SimpleURLEntities */
			ArrayList<TwitterURLEntity> urlEntities = new ArrayList<TwitterURLEntity>();
			for (URLEntity urlEntity : status.getURLEntities()) {
				TwitterURLEntity simpleURLEntity = new TwitterURLEntity();
				simpleURLEntity.setEnd(urlEntity.getEnd());
				simpleURLEntity.setStart(urlEntity.getStart());
				simpleURLEntity.setURL(urlEntity.getURL());
				simpleURLEntity.setDisplayURL(urlEntity.getDisplayURL());
				simpleURLEntity.setExpandedURL(urlEntity.getExpandedURL());
				urlEntities.add(simpleURLEntity);
			}
			TwitterStatusUpdate.setUrlEntities(urlEntities);

			/* Setting SimpleUserMentionEntities */
			ArrayList<TwitterUserMentionEntity> userMentionEntities = new ArrayList<TwitterUserMentionEntity>();
			for (UserMentionEntity userMentionEntity : status
					.getUserMentionEntities()) {
				TwitterUserMentionEntity simpleUserMentionEntity = new TwitterUserMentionEntity();
				simpleUserMentionEntity.setEnd(userMentionEntity.getEnd());
				simpleUserMentionEntity.setStart(userMentionEntity.getStart());
				simpleUserMentionEntity.setId(userMentionEntity.getId());
				simpleUserMentionEntity.setName(userMentionEntity.getName());
				simpleUserMentionEntity.setScreenName(userMentionEntity
						.getScreenName());
				userMentionEntities.add(simpleUserMentionEntity);
			}
			TwitterStatusUpdate.setUserMentionEntities(userMentionEntities);

			/* Setting SimpleMediaEntities */
			ArrayList<TwitterMediaEntity> mediaEntities = new ArrayList<TwitterMediaEntity>();
			for (MediaEntity mediaEntity : status.getMediaEntities()) {
				TwitterMediaEntity simpleMediaEntity = new TwitterMediaEntity();
				simpleMediaEntity.setId(mediaEntity.getId());
				simpleMediaEntity.setType(mediaEntity.getType());
				simpleMediaEntity.setMediaURL(mediaEntity.getMediaURL());
				simpleMediaEntity.setMediaURLHttps(mediaEntity
						.getMediaURLHttps());
				mediaEntities.add(simpleMediaEntity);
			}
			TwitterStatusUpdate.setMediaEntities(mediaEntities);

			/* Setting SimpleUser */
			TwitterUser simpleUser = new TwitterUser();
			simpleUser.setId(status.getUser().getId());
			simpleUser.setScreenName(status.getUser().getScreenName());
			simpleUser.setCreatedAt(status.getUser().getCreatedAt().toString());
			simpleUser.setFollowersCount(status.getUser().getFollowersCount());
			simpleUser.setFriendsCount(status.getUser().getFriendsCount());
			simpleUser.setListedCount(status.getUser().getListedCount());
			simpleUser
					.setProfileImageURL(status.getUser().getProfileImageURL());
			simpleUser.setStatusesCount(status.getUser().getStatusesCount());
			simpleUser.setVerified(status.getUser().isVerified());
			TwitterStatusUpdate.setUser(simpleUser);

			/* Setting SimplePlace */
			if (status.getPlace() != null) {
				TwitterPlace simplePlace = new TwitterPlace();
				simplePlace.setURL(status.getPlace().getURL());
				simplePlace.setStreetAddress(status.getPlace()
						.getStreetAddress());
				simplePlace.setPlaceType(status.getPlace().getPlaceType());
				simplePlace.setName(status.getPlace().getName());
				simplePlace.setFullName(status.getPlace().getFullName());
				simplePlace.setId(status.getPlace().getId());
				simplePlace.setCountry(status.getPlace().getCountry());
				simplePlace.setCountryCode(status.getPlace().getCountryCode());

				ArrayList<TwitterGeoLocation> geoLocationArrayList = new ArrayList<TwitterGeoLocation>();
				GeoLocation[][] geoLocations = status.getPlace()
						.getBoundingBoxCoordinates();
				if (geoLocations != null) {
					for (int i = 0; i < geoLocations.length; i++) {
						for (int j = 0; j < geoLocations[i].length; j++) {
							TwitterGeoLocation simpleGeoLocation = new TwitterGeoLocation();
							simpleGeoLocation.setLatitude(geoLocations[i][j]
									.getLatitude());
							simpleGeoLocation.setLongitude(geoLocations[i][j]
									.getLongitude());
							geoLocationArrayList.add(simpleGeoLocation);
						}
					}
				}
				simplePlace.setBoundingBox(geoLocationArrayList);
				TwitterStatusUpdate.setPlace(simplePlace);
			}
		} catch (Exception exception) {
			logger.error(
					"Exception while converting a Status object to a TwitterStatusUpdate object",
					exception);
			// crisisMailer.sendEmailAlert(exception);
		}

		return TwitterStatusUpdate;
	}
}
