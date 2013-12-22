<?php
require_once dirname(__FILE__) . "/DagManager.php";
$dagManager = new DagManager(5);
$dagManager->run();
