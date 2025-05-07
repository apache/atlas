// @ts-nocheck

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Button,
  Chip,
  IconButton,
  InputBase,
  Paper,
  Slide,
  Stack,
  Typography
} from "@mui/material";
import { cloneDeep, extend } from "@utils/Helper";
import {
  customSortBy,
  extractKeyValueFromEntity,
  isArray,
  isEmpty
} from "@utils/Utils";
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { Link as RouterLink, useLocation, useParams } from "react-router-dom";
import { entityStateReadOnly, graphIcon } from "@utils/Enum";
import {
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon
} from "@mui/icons-material";
import ArrowRightAltIcon from "@mui/icons-material/ArrowRightAlt";
import { CloseIcon, LightTooltip } from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { Link as MUILink } from "@mui/material";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";

const CustomLink = ({
  href,
  status,
  entityColor,
  guid,
  name,
  typeName,
  params
}: any): any => {
  return (
    <li className={status} style={{ listStyle: "numeric" }}>
      <MUILink
        component={RouterLink}
        to={{
          pathname: href,
          search: params.toString() ? params.toString() : ""
        }}
        style={{ color: entityColor }}
        replace={true}
      >
        {name} ({typeName})
      </MUILink>
    </li>
  );
};

const RelationshipLineage = ({ entity }: { entity: Record<string, any> }) => {
  const entityData = cloneDeep(entity);
  const { guid } = useParams();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const svgRef = useRef(null);
  const zoomInButtonRef = useRef(null);
  const zoomOutButtonRef = useRef(null);
  const relationshipSVG = useRef(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [nodeDetails, setNodeDetails] = useState<any>({});
  const [searchTerm, setSearchTerm] = useState("");
  const [zoomId, setZoomId] = useState("");

  const createData = (entityData: Record<string, any>) => {
    let links = [];
    let nodes: Record<string, any> = {};
    if (entityData && entityData.relationshipAttributes) {
      for (const obj in entityData.relationshipAttributes) {
        if (!isEmpty(entityData.relationshipAttributes[obj])) {
          links.push({
            source:
              nodes[entityData.typeName] ||
              (nodes[entityData.typeName] = Object.assign(
                { name: entityData.typeName },
                { value: entityData }
              )),
            target:
              nodes[obj] ||
              (nodes[obj] = Object.assign(
                {
                  name: obj
                },
                { value: entityData.relationshipAttributes[obj] }
              )),
            value: entityData.relationshipAttributes[obj]
          });
        }
      }
    }
    return { nodes: nodes, links: links };
  };

  let graphData = createData(entityData);
  const createGraph = (data) => {
    // Use getBoundingClientRect to get the actual width and height
    const svgElement = svgRef?.current;
    const { width, height } = svgElement
      ? svgElement.getBoundingClientRect()
      : { width: 0, height: 0 };

    let nodes = d3.values(data.nodes);
    let links = data.links;

    var activeEntityColor = "#00b98b",
      deletedEntityColor = "#BB5838",
      defaultEntityColor = "#e0e0e0",
      selectedNodeColor = "#4a90e2";

    var svg = d3
        .select(svgElement)
        .attr("viewBox", `0 0 ${width} ${height}`)
        .attr("enable-background", `new 0 0 ${width} ${height}`),
      node,
      path;

    var container = svg
      .append("g")
      .attr("id", "container")
      .attr("transform", "translate(0,0)scale(1,1)");

    var zoom = d3
      .zoom()
      .scaleExtent([0.1, 4])
      .on("zoom", function () {
        container.attr("transform", d3.event.transform);
      });
    svg.call(zoom).on("dblclick.zoom", null);

    container
      .append("svg:defs")
      .selectAll("marker")
      .data(["deletedLink", "activeLink"]) // Different link/path types can be defined here
      .enter()
      .append("svg:marker") // This section adds in the arrows
      .attr("id", String)
      .attr("viewBox", "-0 -5 10 10")
      .attr("refX", 10)
      .attr("refY", -0.5)
      .attr("orient", "auto")
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .append("svg:path")
      .attr("d", "M 0,-5 L 10 ,0 L 0,5")
      .attr("fill", function (d) {
        return d == "deletedLink" ? deletedEntityColor : activeEntityColor;
      })
      .style("stroke", "none");

    var forceLink = d3
      .forceLink()
      .id(function (d: any) {
        return d.id;
      })
      .distance(function (d) {
        return 100;
      })
      .strength(1);

    var simulation = d3
      .forceSimulation()
      .force("link", forceLink)
      .force("charge", d3.forceManyBody())
      .force("center", d3.forceCenter(width / 2, height / 2));

    update();
    function update() {
      path = container
        .append("svg:g")
        .selectAll("path")
        .data(links)
        .enter()
        .append("svg:path")
        .attr("class", "relationship-link")
        .attr("stroke", function (d) {
          return getPathColor({ data: d, type: "path" });
        })
        .attr("marker-end", function (d) {
          return (
            "url(#" +
            (isAllEntityRelationDeleted({ data: d })
              ? "deletedLink"
              : "activeLink") +
            ")"
          );
        });

      node = container
        .selectAll(".node")
        .data(nodes)
        .enter()
        .append("g")
        .attr("class", "node")
        .on("mousedown", function () {
          d3.event.preventDefault();
        })
        .on("click", function (d) {
          if (d3.event.defaultPrevented) return; // ignore drag
          if (d && d.value && d.value.guid == guid) {
            return;
          }
          setDrawerOpen(true);
          setNodeDetails(d);
        })
        .call(d3.drag().on("start", dragstarted).on("drag", dragged));

      var circleContainer = node.append("g");

      circleContainer
        .append("circle")
        .attr("cx", 0)
        .attr("cy", 0)
        .attr("r", function (d) {
          d.radius = 25;
          return d.radius;
        })
        .attr("fill", function (d: any) {
          if (d && d.value && d.value.guid == guid) {
            if (isAllEntityRelationDeleted({ data: d, type: "node" })) {
              return deletedEntityColor;
            } else {
              return selectedNodeColor;
            }
          } else if (isAllEntityRelationDeleted({ data: d, type: "node" })) {
            return deletedEntityColor;
          } else {
            return activeEntityColor;
          }
        })
        .attr("typename", function (d) {
          return d.name;
        });

      circleContainer
        .append("text")
        .attr("x", 0)
        .attr("y", 0)
        .attr("dy", 25 - 17)
        .attr("text-anchor", "middle")
        .style("font-family", "FontAwesome")
        .style("font-size", function (d) {
          return "25px";
        })
        .text(function (d) {
          var iconObj = graphIcon[d.name];
          if (iconObj && iconObj.textContent) {
            return iconObj.textContent;
          } else {
            if (d && isArray(d.value) && d.value.length > 1) {
              return "\uf0c5";
            } else {
              return "\uf016";
            }
          }
        })
        .attr("fill", function (d) {
          return "#fff";
        });

      var countBox = circleContainer.append("g");

      countBox
        .append("circle")
        .attr("cx", 18)
        .attr("cy", -20)
        .attr("r", function (d) {
          if (isArray(d.value) && d.value.length > 1) {
            return 10;
          }
        });

      countBox
        .append("text")
        .attr("dx", 18)
        .attr("dy", -16)
        .attr("text-anchor", "middle")
        .attr("fill", defaultEntityColor)
        .text(function (d) {
          if (isArray(d.value) && d.value.length > 1) {
            return d.value.length;
          }
        });

      node
        .append("text")
        .attr("x", -15)
        .attr("y", "35")
        .text(function (d) {
          return d.name;
        });

      simulation.nodes(nodes).on("tick", ticked);

      simulation.force("link").links(links);
    }

    function ticked() {
      path.attr("d", function (d) {
        var diffX = d.target.x - d.source.x,
          diffY = d.target.y - d.source.y,
          // Length of path from center of source node to center of target node
          pathLength = Math.sqrt(diffX * diffX + diffY * diffY),
          // x and y distances from center to outside edge of target node
          offsetX = (diffX * d.target.radius) / pathLength,
          offsetY = (diffY * d.target.radius) / pathLength;

        return (
          "M" +
          d.source.x +
          "," +
          d.source.y +
          "A" +
          pathLength +
          "," +
          pathLength +
          " 0 0,1 " +
          (d.target.x - offsetX) +
          "," +
          (d.target.y - offsetY)
        );
      });

      node.attr("transform", function (d) {
        return "translate(" + d.x + "," + d.y + ")";
      });
    }

    function dragstarted(d) {
      d3.event.sourceEvent.stopPropagation();
      if (d && d.value && d.value.guid != guid) {
        if (!d3.event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      }
    }

    function dragged(d) {
      if (d && d.value && d.value.guid != guid) {
        d.fx = d3.event.x;
        d.fy = d3.event.y;
      }
    }

    function getPathColor(options) {
      return isAllEntityRelationDeleted(options)
        ? deletedEntityColor
        : activeEntityColor;
    }

    function isAllEntityRelationDeleted(options) {
      let data = options.data;
      let type = options.type;
      let d = extend(true, {}, data);
      if (d && !isArray(d.value)) {
        d.value = [d.value];
      }

      return (
        d.value.findIndex(function (val) {
          if (type == "node") {
            return (val.entityStatus || val.status) == "ACTIVE";
          } else {
            return val.relationshipStatus == "ACTIVE";
          }
        }) == -1
      );
    }

    if (zoomInButtonRef.current) {
      var zoomInClick = function () {
        var scaleFactor = 1.3;
        zoom.scaleBy(svg.transition().duration(750), scaleFactor);
      };
      d3.select(zoomInButtonRef.current).on("click", zoomInClick);
    }
    if (zoomOutButtonRef.current) {
      var zoomOutClick = function () {
        var scaleFactor = 0.8;
        zoom.scaleBy(svg.transition().duration(750), scaleFactor);
      };
      d3.select(zoomOutButtonRef.current).on("click", zoomOutClick);
    }
  };

  useEffect(() => {
    if (svgRef?.current) {
      createGraph(graphData);
    }
  }, []);

  if (graphData && isEmpty(graphData.links)) {
    return <Typography>No relationship data found</Typography>;
  }

  const updateRelationshipDetails = (options) => {
    let data = options.obj.value;
    let typeName = data.typeName || options.obj.name;
    let searchString = options.searchString;
    let listString = [];
    const getEntityTypelist = (options) => {
      let activeEntityColor = "#4a90e2";
      let deletedEntityColor = "#BB5838";
      const getdefault = (obj) => {
        let options = obj.options;
        let status = entityStateReadOnly[options.entityStatus || options.status]
          ? " deleted-relation"
          : "";
        let nodeGuid = options.guid;
        let entityColor = obj.color;
        let name = obj.name;
        let typeName = options.typeName;
        let keys = Array.from(searchParams.keys());
        for (let i = 0; i < keys.length; i++) {
          if (keys[i] != "searchType") {
            searchParams.delete(keys[i]);
          }
        }
        if (typeName === "AtlasGlossaryTerm") {
          let tempLink = `/glossary/${nodeGuid}`;

          searchParams.set("guid", nodeGuid);
          searchParams.set("gtype", "term");
          searchParams.set("viewType", "term");
          return (
            <CustomLink
              href={tempLink}
              status={status}
              entityColor={entityColor}
              guid={nodeGuid}
              name={name}
              typeName={typeName}
              params={searchParams}
            />
          );
        } else {
          // searchParams.set("tabActive", "relationship");
          return (
            <CustomLink
              href={`/detailPage/${nodeGuid}`}
              status={status}
              entityColor={entityColor}
              guid={nodeGuid}
              name={name}
              typeName={typeName}
              // params={"searchParams"}
              params={""}
            />
          );
        }
      };
      const getWithButton = (obj) => {
        let options = obj.options;
        let status = entityStateReadOnly[options.entityStatus || options.status]
          ? " deleted-relation"
          : "";
        let entityColor = obj.color;
        let name = obj.name;
        let typeName = options.typeName;

        let relationship = obj.relationship || false;
        let entity = obj.entity || false;
        let icon = '<i class="fa fa-trash"></i>';
        let title = "Deleted";
        if (relationship) {
          icon = '<i class="fa fa-long-arrow-right"></i>';
          status = entityStateReadOnly[
            options.relationshipStatus || options.status
          ]
            ? "deleted-relation"
            : "";
          title = "Relationship Deleted";
        }
        return (
          <li className={status} style={{ listStyle: "numeric" }}>
            <MUILink
              component={RouterLink}
              to={`#!/detailPage/${options.guid}?tabActive=relationship`}
              style={{ color: entityColor }}
            >
              {name} ({options.typeName})
            </MUILink>

            <Button
              type="button"
              title={title}
              className="btn btn-sm deleteBtn deletedTableBtn btn-action"
              startIcon={
                relationship ? (
                  <ArrowRightAltIcon sx={{ fontSize: "1.25rem" }} />
                ) : (
                  <LightTooltip title="Deleted">
                    <IconButton
                      aria-label="back"
                      sx={{
                        display: "inline-flex",
                        position: "relative",
                        padding: "4px",
                        marginLeft: "4px",
                        color: (theme) => theme.palette.grey[500]
                      }}
                    >
                      <DeleteOutlineOutlinedIcon sx={{ fontSize: "1.25rem" }} />
                    </IconButton>
                  </LightTooltip>
                )
              }
            ></Button>
          </li>
        );
      };

      const name = options.entityName
        ? options.entityName
        : extractKeyValueFromEntity(options, "displayText").name;
      if (options.entityStatus == "ACTIVE") {
        if (options.relationshipStatus == "ACTIVE") {
          return getdefault({
            color: activeEntityColor,
            options: options,
            name: name
          });
        } else if (options.relationshipStatus == "DELETED") {
          return getWithButton({
            color: activeEntityColor,
            options: options,
            name: name,
            relationship: true
          });
        }
      } else if (options.entityStatus == "DELETED") {
        return getWithButton({
          color: deletedEntityColor,
          options: options,
          name: name,
          entity: true
        });
      } else {
        return getdefault({
          color: activeEntityColor,
          options: options,
          name: name
        });
      }
    };

    var getElement = function (options) {
      const name = options.entityName
        ? options.entityName
        : extractKeyValueFromEntity(options, "displayText").name;
      return getEntityTypelist(options);
    };
    if (isArray(data)) {
      if (data.length > 1) {
        // setSearchNode
      }

      const sortedData = customSortBy(data, ["displayText"]);

      for (const val of sortedData) {
        const { name } = extractKeyValueFromEntity(val, "displayText");
        const valObj = { ...val, entityName: name };

        if (searchString) {
          if (name.search(new RegExp(searchString, "i")) !== -1) {
            listString.push(getElement(valObj));
          } else {
            continue;
          }
        } else {
          listString.push(getElement(valObj));
        }
      }
    } else {
      listString.push(getElement(data));
    }
    return (
      <Stack sx={{ background: "white" }} minHeight={"150px"} maxWidth="400px">
        {/* {listString?.length > 1 && ( */}
        <Paper
          variant="outlined"
          sx={{
            borderRadius: "4px !important",
            border: "1px solid #eeeee",
            color: "black",
            padding: "0 1rem",
            marginBottom: "0.5rem"
          }}
        >
          <Stack>
            <InputBase
              fullWidth
              placeholder="Search Entities..."
              inputProps={{ "aria-label": "search" }}
              value={searchTerm}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                setSearchTerm(e.target.value);
              }}
            />
          </Stack>
        </Paper>
        {/* )} */}

        <Stack gap={"0.875rem"}>{listString}</Stack>
      </Stack>
    );
  };

  return (
    <>
      <Stack display="inline-flex">
        <Stack
          direction="row"
          overflow="auto"
          gap={"8px"}
          top={"-56px"}
          position="relative"
          justifyContent="flex-end"
          display="inline-flex"
          alignItems={"center"}
          width={"50%"}
          marginLeft={"50%"}
        >
          <Stack direction="row" gap="1rem" alignItems="center">
            <Stack direction="row" gap="0.5rem" alignItems="center">
              <Stack direction="row" alignItems="center">
                <LightTooltip title="Active Entity">
                  <ArrowRightAltIcon className="text-color-green" />
                </LightTooltip>
                <Typography lineHeight="31px" className="text-color-green">
                  Active
                </Typography>
              </Stack>
              <Stack direction="row" alignItems="center">
                <LightTooltip title="Deleted Entity">
                  <ArrowRightAltIcon color="error" />
                </LightTooltip>
                <Typography lineHeight="31px" color="error">
                  Deleted
                </Typography>
              </Stack>
            </Stack>
            <Stack direction="row">
              <IconButton
                ref={zoomInButtonRef}
                size="small"
                id="zoom_in"
                title="Zoom In"
              >
                <ZoomInIcon />
              </IconButton>
              <IconButton
                ref={zoomOutButtonRef}
                size="small"
                id="zoom_out"
                title="Zoom Out"
              >
                <ZoomOutIcon />
              </IconButton>
            </Stack>
          </Stack>
        </Stack>
        <Stack
          className="relationship-box"
          ref={relationshipSVG}
          style={{
            width: "100%",
            height: "400px",
            position: "relative",
            top: "-60px"
          }}
          data-id="relationshipSVG"
          data-cy="relationshipSVG"
        >
          <Stack
            marginTop="10px"
            style={{
              display: drawerOpen ? "block" : "none",
              position: drawerOpen ? "absolute" : "relative",
              left: drawerOpen ? 0 : "unset",
              top: drawerOpen ? 12 : "unset",
              zIndex: drawerOpen ? 99999 : "unset",
              background: "white"
            }}
          >
            <Slide direction="right" in={drawerOpen}>
              <Stack>
                <Stack
                  direction="row"
                  justifyContent={"center"}
                  padding={"12px 16px"}
                  borderRadius="8px 8px 0 0"
                  style={{ background: "#4a90e2" }}
                >
                  <Typography
                    variant="h6"
                    color="white"
                    flex={1}
                    fontWeight="600"
                  >
                    {nodeDetails?.name}
                  </Typography>
                  <Button
                    onClick={() => setDrawerOpen(false)}
                    size="small"
                    sx={{ padding: 0, minWidth: "24px", color: "white" }}
                  >
                    <CloseIcon />
                  </Button>
                </Stack>

                <Stack
                  maxHeight={"350px"}
                  gap={1}
                  padding="0.875rem"
                  textAlign="left"
                  borderRadius="0 0 8px 8px"
                  border="1px solid rgba(0,0,0,0.12)"
                  sx={{ background: "white", overflowY: "auto" }}
                >
                  {!isEmpty(nodeDetails?.value) &&
                    updateRelationshipDetails({
                      obj: nodeDetails,
                      searchString: searchTerm
                    })}
                </Stack>
              </Stack>
            </Slide>
          </Stack>
          <svg
            ref={svgRef}
            width="100%"
            height="100%"
            // viewBox="0 0 854 330"
            enableBackground="new 0 0 854 330"
            xmlSpace="preserve"
          ></svg>
        </Stack>
      </Stack>
    </>
  );
};

export default RelationshipLineage;
