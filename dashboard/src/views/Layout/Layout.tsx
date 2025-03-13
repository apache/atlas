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

import { useEffect, useState } from "react";
import SideBarBody from "../SideBar/SideBarBody";
import { Navigate, useLocation } from "react-router-dom";
import Statistics from "@views/Statistics/Statistics";
import CustomModal from "@components/Modal";
import About from "./About";
import { useIdleTimer } from "react-idle-timer";
import { Typography } from "@mui/material";
import { getBaseUrl } from "@utils/Utils";
import { useAppSelector } from "@hooks/reducerHook";

const Layout: React.FC = () => {
  const location = useLocation();
  const { sessionObj = "" }: any = useAppSelector(
    (state: any) => state.session
  );
  const { data } = sessionObj || {};
  const key = "atlas.session.timeout.secs";
  const [openModal, setOpenModal] = useState(false);
  const [openAboutModal, setOpenAboutModal] = useState(false);
  const [openSessionModal, setOpenSessionModal] = useState(false);
  const [timer, setTimer] = useState<number>(0);

  const handleOpenModal = () => setOpenModal(true);
  const handleCloseModal = () => setOpenModal(false);
  const handleOpenAboutModal = () => setOpenAboutModal(true);
  const handleCloseAboutModal = () => setOpenAboutModal(false);
  const handleCloseSessionModal = () => setOpenAboutModal(false);

  const timeout = 1000 * (data?.[key] > 0 ? data?.[key] : 900);
  const promptBeforeIdle = 1000 * 15;
  const onPrompt = () => {
    setOpenSessionModal(true);
    setTimer(promptBeforeIdle);
  };

  const onIdle = () => {
    setOpenSessionModal(false);
    handleLogout();
    setTimer(0);
  };
  const onActive = () => {
    setOpenSessionModal(false);
    setTimer(0);
  };

  const { getRemainingTime, isPrompted, activate } = useIdleTimer({
    timeout,
    promptBeforeIdle,
    onPrompt,
    onIdle,
    onActive,
    crossTab: true,
    throttle: 1000,
    eventsThrottle: 1000,
    startOnMount: true
  });
  const handleStillHere = () => {
    setOpenSessionModal(false);
    activate();
  };

  useEffect(() => {
    const interval = setInterval(() => {
      if (isPrompted()) {
        setTimer(Math.ceil(getRemainingTime() / 1000));
      }
    }, 1000);
    return () => {
      clearInterval(interval);
    };
  }, [getRemainingTime, isPrompted]);

  const handleLogout = () => {
    localStorage.setItem("atlas_ui", "beta");
    let path = getBaseUrl(window.location.pathname);
    window.location.href = path + "/logout.html";
    handleCloseSessionModal();
  };

  return (
    <>
      {openSessionModal && (
        <CustomModal
          open={openSessionModal}
          onClose={handleCloseSessionModal}
          title={"Your session is about to expire"}
          button1Label="Logout"
          button1Handler={handleLogout}
          button2Label="Stay-Signed-in"
          button2Handler={handleStillHere}
        >
          <Typography fontWeight="600" color="text.secondary">
            Your session is about to expire
          </Typography>
          <br />
          <Typography fontWeight="600" color="text.secondary">
            You will be logged out in: {timer} secs.
          </Typography>
        </CustomModal>
      )}
      {(location.pathname === "/" || location.pathname.includes("!")) && (
        <Navigate to={"/search"} replace={true} />
      )}
      <div className="row">
        <div className="column layout-sidebar">
          <SideBarBody
            loading={false}
            handleOpenModal={handleOpenModal}
            handleOpenAboutModal={handleOpenAboutModal}
          />
        </div>
      </div>
      {openModal && (
        <Statistics open={openModal} handleClose={handleCloseModal} />
      )}
      {openAboutModal && (
        <CustomModal
          open={openAboutModal}
          onClose={handleCloseAboutModal}
          title="Apache Atlas"
          button2Label="OK"
          button2Handler={handleCloseAboutModal}
          button1Handler={undefined}
        >
          <About />
        </CustomModal>
      )}
    </>
  );
};

export default Layout;
