<?php
/**
 * @file DagExecComponents.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/19 17:38:43
 * @brief
 *
 **/
 
require_once dirname(dirname(__FILE__)).'/components/ModelExecHandler.php';

class DagExecComponent {
	public function __construct($evid) {
		$this->modelhandler = new ModelExecHandler($evid);  
        $this->evid = $evid;
        $this->distinct = array();
	}
	
	public function constructExecInfo($queue) {
		$submitTime = time();
		$etlexec = $this->modelhandler->buildEtlExecution($submitTime);
		$this->eeid = $etlexec->eeid();
		$index = 0;
		foreach ($queue as $flows) {
			self::fillExecution($this->eeid, $flows, $index);
			$index++;
		}
	}
	
	private function fillExecution($eeid, $flows, $index) {
		$type = null;
		$args = array();
		switch ($flows[0]->type) {
			case ConstDef::SINGLEMAP:	
			case ConstDef::UNION:
			case ConstDef::JOIN:
				foreach ($flows as $flow) {
					foreach ($flow->flowattr->args as $key => $value) {
						$args[$key] = $value;
					}
				}
				$type = ConstDef::HADOOP;
				break;
			case ConstDef::ANTISPAM:
				$type = ConstDef::LOCAL;
				break; 
		}
		$execution = $this->modelhandler->buildExecution($eeid, $type, $args);
		$exid = $execution->exid();
		$this->qindex2exec[$index] = $exid;
		self::fillExecFlow($eeid, $exid, $flows);
	}
	
	private function fillExecFlow($eeid, $exid, $flows) {
		switch ($flows[0]->type) {
			case ConstDef::SINGLEMAP:
			case ConstDef::ANTISPAM:
				$fid = $flows[0]->fid;
				$execflow = $this->modelhandler->buildExecFlow($eeid, $fid, $exid);
				$this->fid2efid[$fid] = $execflow->efid();
				self::fillExecData($eeid, $exid, $flows[0]);
				break;
			case ConstDef::UNION:
			case ConstDef::JOIN:
				foreach ($flows as $flow) {
					$fid = $flow->fid;
					$execflow = $this->modelhandler->buildExecFlow($eeid, $fid, $exid);
					$this->fid2efid[$fid] = $execflow->efid();
					self::fillExecData($eeid, $exid, $flow);
				}
				break;
		}
	}
	
	private function fillExecData($eeid, $exid, $flow) {
		foreach ($flow->startnodes as $startnode) {
			$did = $startnode->did;
			if (in_array($did, $this->distinct)) {
				continue;
			}
			$execdata = $this->modelhandler->buildExecData($eeid, $did, $exid);
			$did2edid[$did] = $execdata->edid();
			$this->distinct[] = $did;	
		}
		foreach ($flow->endnodes as $endnode) {
			$did = $endnode->did;
			if (in_array($did, $this->distinct)) {
				continue;
			}
			$execdata = $this->modelhandler->buildExecData($eeid, $did, $exid);
			$did2edid[$did] = $execdata->edid();
			$this->distinct[] = $did;
		}
	}
	
	private $distinct;
	private $qindex2exec;
	private $fid2efid;
	private $did2edid;
	private $modelhandler;
	private $evid;
	private $eeid;
};
 
