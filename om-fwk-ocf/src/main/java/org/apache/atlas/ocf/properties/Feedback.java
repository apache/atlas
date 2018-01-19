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

package org.apache.atlas.ocf.properties;


/**
 * Feedback contains the comments, tags, ratings and likes that consumers of the asset have created.
 */
public class Feedback extends AssetPropertyBase
{
    /*
     * Lists of objects that make up the feedback on the asset.
     */
    private  InformalTags      informalTags = null;
    private  Likes             likes = null;
    private  Ratings           ratings = null;
    private  Comments          comments = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - description of the asset that this feedback is attached to.
     * @param informalTags - list of tags for the asset.
     * @param likes - list of likes (one object per person liking the asset) for the asset.
     * @param ratings - list of ratings that people have given the asset - one Rating object for each person's rating.
     * @param comments - list of comments for the asset.
     */
    public Feedback(AssetDescriptor parentAsset, InformalTags informalTags, Likes likes, Ratings ratings, Comments comments)
    {
        super(parentAsset);
        this.informalTags = informalTags;
        this.likes = likes;
        this.ratings = ratings;
        this.comments = comments;
    }


    /**
     * Copy/clone constructor - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the feedback clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this feedback is attached to.
     * @param templateFeedback - template object to copy.
     */
    public Feedback(AssetDescriptor  parentAsset, Feedback   templateFeedback)
    {
        super(parentAsset, templateFeedback);

        /*
         * Only create a child object if the template is not null.
         */
        if (templateFeedback != null)
        {
            InformalTags      templateInformalTags = templateFeedback.getInformalTags();
            Likes             templateLikes = templateFeedback.getLikes();
            Ratings           templateRatings = templateFeedback.getRatings();
            Comments          templateComments = templateFeedback.getComments();

            if (templateInformalTags != null)
            {
                this.informalTags = templateInformalTags.cloneIterator(parentAsset);
            }

            if (templateLikes != null)
            {
                this.likes = templateLikes.cloneIterator(parentAsset);
            }

            if (templateRatings != null)
            {
                this.ratings = templateRatings.cloneIterator(parentAsset);
            }

            if (templateComments != null)
            {
                this.comments = templateComments.cloneIterator(parentAsset);
            }
        }

    }


    /**
     * Returns a copy of the information tags for the asset in an iterator.  This iterator can be used to step
     * through the tags once.  Therefore call getInformalTags() for each scan of the asset's tags.
     *
     * @return InformalTags - tag list
     */
    public InformalTags getInformalTags()
    {
        if (informalTags == null)
        {
            return informalTags;
        }
        else
        {
            return informalTags.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Returns a copy of the likes for the asset in an iterator.  This iterator can be used to step
     * through the list of like once.  Therefore call getLikes() for each scan of the asset's like objects.
     *
     * @return Likes - like object list
     */
    public Likes getLikes()
    {
        if (likes == null)
        {
            return likes;
        }
        else
        {
            return likes.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Returns a copy of the ratings for the asset in an iterator.  This iterator can be used to step
     * through the ratings once.  Therefore call getRatings() for each scan of the asset's ratings.
     *
     * @return Ratings - rating list
     */
    public Ratings getRatings()
    {
        if (ratings == null)
        {
            return ratings;
        }
        else
        {
            return ratings.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Returns a copy of the comments for the asset in an iterator.  This iterator can be used to step
     * through the comments once.  Therefore call getComments() for each scan of the asset's comments.
     *
     * @return Comments - comment list
     */
    public Comments getComments()
    {
        if (comments == null)
        {
            return comments;
        }
        else
        {
            return comments.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Feedback{" +
                "informalTags=" + informalTags +
                ", likes=" + likes +
                ", ratings=" + ratings +
                ", comments=" + comments +
                '}';
    }
}