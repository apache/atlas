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

import { useState } from "react";
import SideBarBody from "../SideBar/SideBarBody";
import { Navigate, useLocation } from "react-router-dom";
import Statistics from "@views/Statistics/Statistics";
import CustomModal from "@components/Modal";
import About from "./About";

const Layout: React.FC = () => {
  const location = useLocation();
  const [openModal, setOpenModal] = useState(false);
  const [openAboutModal, setOpenAboutModal] = useState(false);

  const handleOpenModal = () => setOpenModal(true);
  const handleCloseModal = () => setOpenModal(false);
  const handleOpenAboutModal = () => setOpenAboutModal(true);
  const handleCloseAboutModal = () => setOpenAboutModal(false);
  return (
    <>
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
