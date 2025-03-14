import { getBaseApiUrl } from "./commonApiUrl";

const getSavedSearchUrl = () => {
  var termUrl = getBaseApiUrl("urlV2") + `/search/saved`;
  return termUrl;
};

export { getSavedSearchUrl };
