<?php
/**
 * @file ModelExecHandler.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/19 18:05:23
 * @brief
 *
 **/
 
require_once dirname(dirname(__FILE__)).'/models/DBEtlExecution.php';
require_once dirname(dirname(__FILE__)).'/models/DBExecution.php';
require_once dirname(dirname(__FILE__)).'/models/DBExecData.php';
require_once dirname(dirname(__FILE__)).'/models/DBExecFlow.php';
 
 class ModelExecHandler {
 	public function __construct($evid) {
		$this->evid = $evid;
 	}
 	
 	public function buildEtlExecution($submitTime) {
 		$startTime = 0;
 		$endTime = 0;
 		$status = 0;
 		$taskid = 0;
 		$execution = DBEtlExecution::insertEltExecution($this->evid, $submitTime, $startTime, $endTime, $status, $taskid);
 		if ($execution === null) {
 			throw new Exception("Failed To Insert Etl Execution");
 		}
 		return $execution;
 	}
 	
 	public function updateEtlExecution($eeid, $key, $value) {
 		$execution = DBEtlExecution::getById($eeid);
 		if ($execution === null) {
 			throw new Exception("Failed To Get Etl By eeid");
 		}
 		switch ($key) {
 			case self::STARTTIME:
 				$execution->setStartTime($value);
 				break;
 			case self::ENDTIME:
 				$execution->setEndTime($value);
 				break;
 			case self::STATUS:
 				$execution->setStatus($value);
 				break;
 			case self::TASKID:
 				$execution->setTaskid($value);
 				break;
 		}
 		if (!$execution->update()) {
 			throw new Exception("Failed To Get Etl By eeid");
 		}
 	}
 	
 	public function updateEtlExecutionArray($eeid, $kvs) {
 		$execution = DBEtlExecution::getById($eeid);
 		if ($execution === null) {
 			throw new Exception("Failed To Get Etl By eeid");
 		}
 		foreach ($kvs as $key => $value) {
 			switch ($key) {
 				case self::STARTTIME:
 					$execution->setStartTime($value);
 					break;
 				case self::ENDTIME:
 					$execution->setEndTime($value);
 					break;
 				case self::STATUS:
 					$execution->setStatus($value);
 					break;
 				case self::TASKID:
 					$execution->setTaskid($value);
 					break;
 			}
 		}
 		if (!$execution->update()) {
 			throw new Exception("Failed To Get Etl By eeid");
 		}
 	}
 	
 	public function buildExecution($eeid, $type, $args) {
 		$status = 0;
 		$log = "";
 		$link = "";
 		$realtype = null;
 		switch ($type) {
 			case ConstDef::HADOOP:
 				$realtype = DBExecution::TYPE_FLOW_MR;
 				break;
 			case ConstDef::LOCAL:
 				$realtype = DBExecution::TYPE_USERDEF;
 				break;
 		}
 		$execution = DBExecution::insertExecution($eeid, $realtype, $args, $log, $link, $status);
 		if ($execution === null) {
 			throw new Exception("Failed To Insert Etl Execution");
 		}
 		return $execution;
 	}
 	
 	public function updateExecution($exid, $key, $value) {
 		$execution = DBEtlExecution::getById($exid);
 		if ($execution === null) {
 			throw new Exception("Failed To Get Execution By eeid");
 		}
 		switch ($key) {
 			case self::LOG:
 				$execution->setLog($value);
 				break;
 			case self::LINK:
 				$execution->setLink($value);
 				break;
 			case self::STATUS:
 				$execution->setStatus($value);
 				break;
 		}
 		if (!$execution->update()) {
 			$info = "Failed To Update Execution: ";
 			$info = $info . $key . " = " . $value;
 			throw new Exception($info);
 		}
 	}
 	
 	public function updateExecutionArray($exid, $kvs) {
 		$execution = DBEtlExecution::getById($exid);
 		if ($execution === null) {
 			throw new Exception("Failed To Get Execution By eeid");
 		}
 		foreach ($kvs as $key => $value) {
 			switch ($key) {
 				case self::LOG:
 					$execution->setLog($value);
 					break;
 				case self::LINK:
 					$execution->setLink($value);
 					break;
 				case self::STATUS:
 					$execution->setStatus($value);
 					break;
 			}
 		}
 		if (!$execution->update()) {
 			$info = "Failed To Update Execution: ";
 			$kvinfo = "";
 			foreach ($kvs as $key => $value) {
 				$kvinfo = $key . $value;
 			}
 			$info = $info . $kvinfo;
 			throw new Exception($info);
 		}
 	}
 	
 	public function buildExecData($eeid, $did, $exid) {
 		$status = 0;
 		$execdata = DBExecData::insertExecData($eeid, $did, $exid, $status);
 		if ($execdata === null) {
 			throw new Exception("Failed To Insert Exec Data");
 		}
 		return $execdata;
 	}
 	
 	public function updateExecDataStatus($edid, $status) {
 		$execdata = DBExecData::getById($edid);
 		if ($execdata === null) {
 			$info = "Failed To Get Exec Data" . $edid;
 			throw new Exception($info);
 		}
 		$execdata->setStatus($status);
 		if (!$execdata->update()) {
 			$info = "Failed To Update Exec Data" . $edid;
 			throw new Exception($info);
 		}
 	}
 	
 	public function buildExecFlow($eeid, $fid, $exid) {
 		$status = 0;
 		$execflow = DBExecFlow::insertExecFlow($eeid, $fid, $exid, $status);
 		if ($execflow === null) {
 			throw new Exception("Failed To Insert Exec Flow");
 		}
 		return $execflow;
 	}
 	
 	public function updateExecFlowStatus($efid, $status) {
 		$execflow = DBExecFlow::getById($efid);
 		if ($execflow === null) {
 			$info = "Failed To Get Exec Flow" . $efid;
 			throw new Exception($info);
 		}
 		$execflow->setStatus($status);
 		if (!$execflow->update()) {
 			$info = "Failed To Update Exec Flow" . $efid;
 			throw new Exception($info);
 		}
 	}
 	
 	const STARTTIME = "starttime";
 	const ENDTIME = "endtime";
 	const STATUS = "status";
 	const TASKID = "taskid";
 	
 	const ARGS = "args";
 	const LOG = "log";
 	const LINK = "link";
 	
 	private $evid;
 	private $etl_execution;
 	private $execution;
 	private $exec_data;
 	private $exec_flow;
 }
