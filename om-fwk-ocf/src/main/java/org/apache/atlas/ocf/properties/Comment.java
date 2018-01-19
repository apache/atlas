/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.ocf.properties;


/**
 * Stores information about a comment connected to an asset.  Comments provide informal feedback to assets
 * and can be added at any time.
 *
 * Comments have the userId of the person who added the feedback, along with their comment text.
 *
 * Comments can have other comments attached.
 *
 * The content of the comment is a personal statement (which is why the user's id is in the comment)
 * and there is no formal review of the content.
 */
public class Comment extends ElementHeader
{
    /*
     * Attributes of a Comment
     */
    private CommentType  commentType    = null;
    private String       commentText    = null;
    private String       user           = null;
    private Comments     commentReplies = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset     - descriptor for parent asset
     * @param type            - details of the metadata type for this properties object
     * @param guid            - String - unique id
     * @param url             - String - URL
     * @param classifications - list of classifications
     * @param commentType     - enum describing the type of the comment
     * @param commentText     - comment text String
     * @param user            - String - user id of the person who created the comment. Null means the user id is not known.
     * @param commentReplies  - Nested list of comments replies
     */
    public Comment(AssetDescriptor parentAsset,
                   ElementType     type,
                   String          guid,
                   String          url,
                   Classifications classifications,
                   CommentType     commentType,
                   String          commentText,
                   String          user,
                   Comments        commentReplies)
    {
        super(parentAsset, type, guid, url, classifications);

        this.commentType    = commentType;
        this.commentText    = commentText;
        this.user           = user;
        this.commentReplies = commentReplies;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset     - descriptor for parent asset
     * @param templateComment - element to copy
     */
    public Comment(AssetDescriptor parentAsset, Comment templateComment)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateComment);

        if (templateComment != null)
        {
            /*
             * Copy the values from the supplied comment.
             */
            commentType = templateComment.getCommentType();
            user        = templateComment.getUser();
            commentText = templateComment.getCommentText();

            Comments templateCommentReplies = templateComment.getCommentReplies();
            if (templateCommentReplies != null)
            {
                /*
                 * Ensure comment replies has this object's parent asset, not the template's.
                 */
                commentReplies = templateCommentReplies.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Return an enum that describes the type of comment.
     *
     * @return CommentType enum
     */
    public CommentType getCommentType()
    {
        return commentType;
    }


    /**
     * Return the user id of the person who created the comment.  Null means the user id is not known.
     *
     * @return String - commenting user
     */
    public String getUser()
    {
        return user;
    }


    /**
     * Return the comment text.
     *
     * @return String - commentText
     */
    public String getCommentText()
    {
        return commentText;
    }


    /**
     * Return an iterator of the replies to this comment - null means no replies are available.
     *
     * @return Comments - comment replies iterator
     */
    public Comments getCommentReplies()
    {
        if (commentReplies == null)
        {
            return commentReplies;
        }
        else
        {
            return commentReplies.cloneIterator(super.getParentAsset());
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
        return "Comment{" +
                "commentText='" + commentText + '\'' +
                ", user='" + user + '\'' +
                ", commentReplies=" + commentReplies +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}