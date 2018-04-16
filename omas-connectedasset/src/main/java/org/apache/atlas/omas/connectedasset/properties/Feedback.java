/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.omas.connectedasset.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Feedback contains the comments, tags, ratings and likes that consumers of the asset have created.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Feedback extends PropertyBase
{
    /*
     * Lists of objects that make up the feedback on the asset.
     */
    private List<InformalTag> informalTags = null;
    private List<Like>        likes        = null;
    private List<Rating>      ratings      = null;
    private List<Comment>     comments     = null;


    /**
     * Default Constructor
     */
    public Feedback()
    {
        super();
    }


    /**
     * Copy/clone constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the feedback clone to point to the
     * asset clone and not the original asset.
     *
     * @param templateFeedback - template object to copy.
     */
    public Feedback(Feedback   templateFeedback)
    {
        super(templateFeedback);

        /*
         * Only create a child object if the template is not null.
         */
        if (templateFeedback != null)
        {
            List<InformalTag>      templateInformalTags = templateFeedback.getInformalTags();
            List<Like>             templateLikes = templateFeedback.getLikes();
            List<Rating>           templateRatings = templateFeedback.getRatings();
            List<Comment>          templateComments = templateFeedback.getComments();

            if (templateInformalTags != null)
            {
                this.informalTags = new ArrayList<>(templateInformalTags);
            }

            if (templateLikes != null)
            {
                this.likes = new ArrayList<>(templateLikes);
            }

            if (templateRatings != null)
            {
                this.ratings = new ArrayList<>(templateRatings);
            }

            if (templateComments != null)
            {
                this.comments = new ArrayList<>(templateComments);
            }
        }

    }


    /**
     * Returns a copy of the information tags for the asset in an iterator.  This iterator can be used to step
     * through the tags once.  Therefore call getInformalTags() for each scan of the asset's tags.
     *
     * @return InformalTags - tag list
     */
    public List<InformalTag> getInformalTags()
    {
        if (informalTags == null)
        {
            return informalTags;
        }
        else
        {
            return new ArrayList<>(informalTags);
        }
    }


    /**
     * Set up the informal tags for the asset.
     *
     * @param informalTags - list of tags.
     */
    public void setInformalTags(List<InformalTag> informalTags) { this.informalTags = informalTags; }


    /**
     * Returns a copy of the likes for the asset in an iterator.  This iterator can be used to step
     * through the list of like once.  Therefore call getLikes() for each scan of the asset's like objects.
     *
     * @return Likes - like object list
     */
    public List<Like> getLikes()
    {
        if (likes == null)
        {
            return likes;
        }
        else
        {
            return new ArrayList<>(likes);
        }
    }


    /**
     * Set up the list of likes (one object per person liking the asset) for the asset.
     *
     * @param likes - list of likes
     */
    public void setLikes(List<Like> likes) { this.likes = likes; }


    /**
     * Returns a copy of the ratings for the asset in an iterator.  This iterator can be used to step
     * through the ratings once.  Therefore call getRatings() for each scan of the asset's ratings.
     *
     * @return Ratings - rating list
     */
    public List<Rating> getRatings()
    {
        if (ratings == null)
        {
            return ratings;
        }
        else
        {
            return new ArrayList<>(ratings);
        }
    }


    /**
     * Set the list of ratings that people have given the asset.
     *
     * @param ratings - list of ratings - one Rating object for each person's rating.
     */
    public void setRatings(List<Rating> ratings) {
        this.ratings = ratings;
    }


    /**
     * Returns a copy of the comments for the asset in an iterator.  This iterator can be used to step
     * through the comments once.  Therefore call getComments() for each scan of the asset's comments.
     *
     * @return Comments - comment list
     */
    public List<Comment> getComments()
    {
        if (comments == null)
        {
            return comments;
        }
        else
        {
            return new ArrayList<>(comments);
        }
    }


    /**
     * Adds the list of comments for the asset.
     *
     * @param comments - comments list.
     */
    public void setComments(List<Comment> comments) { this.comments = comments; }
}