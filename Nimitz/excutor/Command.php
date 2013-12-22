<?php
/**
 * @file Command.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/12 15:50:15
 * @brief
 *
 **/

 class BaseCommand {
 	abstract public function isNeed() {}
 	abstract public function excute($sortedflow) {}
 	abstract public function getType() {}
 	abstract public function getFiles() {}
 	
 	protected $sortedflows;
 	protected $index;
 	protected $dag;
 	protected $files;
};
 
class HadoopCommand extends BaseCommand {
	public function isNeed() {
		return $this->sortedflows[$this->index][0]->type !== ConstDef::ANTISPAM;
	}
	public function getType() {
		return ConstDef::HADOOP;
	}
	public function getFiles() {

	}

	public function excute($sortedflow) {
		$cmd = "HADOOP_CLASSPATH=; for jar in {LIB_JARS_PATH}/../lib.new/*.jar ; do HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$jar;done; export HADOOP_CLASSPATH && {HADOOP_HOME}/bin/hadoop bistreaming ";
		$cmd = $cmd . "-D mapred.job.name={JOB_NAME} ";
		if (self::isNeedPapiRead($sortedflow[0]) === true) {
			$cmd = $cmd . "-D udw.mapred.input.info={WORK_PATH}/input.schema.out ";
		}
		if (self::isNeedPapiWrite($sortedflow[0]) === true) {
			$cmd = $cmd . "-D udw.mapred.output.info={WORK_PATH}/output.schema.out ";
		}
		$cmd = $cmd . "mapred.output.compress = true ";
		$cmd = $cmd . "mapred.output.compression.codec = org.apache.hadoop.io.compress.LzoCodec ";
		$cmd = $cmd . "udw.mapred.streaming.separator = 0x0 ";
		$cmd = $cmd . "-D hadoop.job.ugi=" . $dag->dagattr->cluster->user . "," . $dag->dagattr->cluster->passwd . " "; 
		$cmd = $cmd . "-D mapred.job.queue.name=" . $dag->dagattr->cluster->queue;
		$cmd = $cmd . "-jt " . $dag->dagattr->cluster->jobtracker . " -fs " . $dag->dagattr->cluster->fsname;
		
		$cmd = $cmd . "-input \"/app/ns/udw/release/etl/etl_framework_temp/input\" ";
		$cmd = $cmd . "-output " . $dag->dagattr->cluster->logfsname . "\"/app/ns/udw/release/etl/etl_framework_temp/output{JOB_NAME}\" ";
		$cmd = $cmd . "-mapper \"./run.sh mapper -c mapper.json\" ";
		$cmd = $cmd . "-file {WORK_PATH}/mapper.json ";
		if (self::isNeedReducer($sortedflow[0]) === true) {
			$cmd = $cmd . "-reducer \"./run.sh reducer -c reducer.json\" ";
			$cmd = $cmd . "-file {WORK_PATH}/reducer.json ";
			if (array_key_exists(ConstDef::REDUCERNUM, $sortedflow[0]->flowattr->args)) {
				$cmd = $cmd . "-numReduceTasks ". $sortedflow[0]->flowattr->args[ConstDef::REDUCERNUM];
			}
		} else {
			$cmd = $cmd . "-reducer NONE ";
		}
	}
	
	public function isNeedPapiRead($flow) {
		$isNeed = false;
		foreach ($flow->startnodes as $startnode) {
			if ($startnode->type === ConstDef::MV || $startnode->type === ConstDef::EVENT || $startnode->type === ConstDef::BIGEVENT) {
 				$isNeed = true;
 				break;
 			}
		}
		return $isNeed;
	}
	
	public function isNeedPapiWrite($flow) {
		$isNeed = false;
		foreach ($flow->endnodes as $endnode) {
			if ($endnode->type === ConstDef::MV || $endnode->type === ConstDef::EVENT || $endnode->type === ConstDef::BIGEVENT) {
 				$isNeed = true;
 				break;
 			}
		}
		return $isNeed;
	}
	
	public function isNeedReducer($flow) {
		return $flow->type === ConstDef::JOIN;	
	}
	
	public function getPapiInputOtherFormat($sortedflow) {
		$cmd = "";
		$index = 0;
		foreach ($sortedflow as $flow) {
			foreach ($flow->startnodes as $startnode) {
				if (!($startnode instanceof Log)) {
					continue;
				}
				switch ($startnode->type) {
					case ConstDef::COMBINELOG:
						$cmd = 	"-D udw.mapred.input.other.format." . $index . "=" . self::INPUTFORMAT_COMBINE_SEQ;
						break;
					case ConstDef::LOG:
						$cmd = "-D udw.mapred.input.other.format." . $index . "=LOG";
						break;
					case ConstDef::COMBINETEXTLOG:
						$cmd = "-D udw.mapred.input.other.format." . $index . "=" . self::INPUTFORMAT_COMBINE_TEXT;
						break;
					case ConstDef::TEXTLOG:
						$cmd = "-D udw.mapred.input.other.format." . $index . "=" . self::INPUTFORMAT_TEXT;
						break;
				}
				$cmd = $cmd . "-D udw.mapred.input.other.file." . $index . "=";
				$logpath = $startnode->getLogPath();
				$logfreq = $startnode->getLogFreq();
				if ((int)$dagattr->freq === Dag::FREQDAY) {
 					$path = "/{DAY}/[^@]*/[^@]*/*/*/*";
 				} else if ((int)$dagattr->freq === Dag::FREQHOUR) {
 					$path = "/{DAY}/{HOUR}*/[^@]*/[^@]*/*/*";
 				} else {
 					$times = (int)$dagattr->freq / (int)$logfreq;
 				} 
			}
		}
	}

	const INPUTFORMAT_TEXT = "org.apache.hadoop.mapred.TextInputFormat";
	const INPUTFORMAT_COMBINE_TEXT = "org.apache.hadoop.mapred.CombineTextInputFormat";
	const INPUTFORMAT_SEQ = "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat";
	const INPUTFORMAT_COMBINE_SEQ = "org.apache.hadoop.mapred.CombineSequenceFileAsBinaryInputFormat";
	const INPUTFORMAT_NEW_PAPI = "com.baidu.udw.mapred.MultiTableInputFormat";
	const OUTPUTFORMAT_TEXT = "org.apache.hadoop.mapred.TextOutputFormat";
	const OUTPUTFORMAT_NEW_PAPI = "com.baidu.udw.mapred.MultiTableOutputFormat";	 	
};
 
class AntispamCommand extends BaseCommand {
	public function getType() {
		return ConstDef::LOCAL;
	}
	
	public function isNeed() {
		$ret = false;
		if ($this->sortedflows[$this->index][0]->type === ConstDef::ANTISPAM) {
			$ret = true;
		} else {
			$ret = false;
		}
		return $ret;
	}
	
	public function excute() {
		$flowattr = $this->sortedflows[$this->index]->flowattr;
		if ($flowattr->shfilepath === null) {
			throw new Exception("Sh file Path Not Exists, but it is antispam");
		}
		$files[] = $flowattr->shfilepath;
		$shname = $flowattr->shfile;
		if ($shname === null) {
			throw new Exception("Sh file Not Exists, but it is antispam");
		}
		$cmd = "sh " . $shname . " {DAY} {HOUR} {MIN} {WORK_PATH} {HADOOP_HOME} {LIB_JARS_PATH} {JOB_NAME}";
		$cmd = $cmd . $cluster->queue . $cluster->user . "," . $cluster->passwd . " " . JobConstDef::ZKADDRESS . JobConstDef::ZKPATH;
		return $cmd;
	}
};
 
 class MapperJsonCommand extends BaseCommand {
 	public function getType() {
		return ConstDef::LOCAL;
	}
	
	public function isNeed() {
		$ret = false;
		if ($this->sortedflows[$this->index][0]->type !== ConstDef::ANTISPAM) {
			$ret = true;
		} else {
			$ret = false;
		}
		return $ret;
	}
	
	public function excute($sortedflow) {
		$mapperjson = array();
		foreach ($sortedflow as $sflow) {
			if ($sflow->type !== ConstDef::JOIN) {
				foreach ($sflow->startnodes as $startnode) {
					$inputjson = array();
					$inputjson[ConstDef::NAME] = $startnode->name;
					switch ($startnode->type) {
						case ConstDef::TEXTLOG:
							$inputjson[ConstDef::TYPE] = ConstDef::LOG;
							$inputjson[ConstDef::PRODUCT] = $startnode->product;
							$inputjson[ConstDef::HDFSPATH] = $startnode->hdfspath;
							$inputjson[ConstDef::ORIGINAL] = true;
							break;
						case ConstDef::LOG:
							$inputjson[ConstDef::TYPE] = ConstDef::LOG;
							$inputjson[ConstDef::PRODUCT] = $startnode->product;
							$inputjson[ConstDef::HDFSPATH] = $startnode->hdfspath;
							$inputjson[ConstDef::ORIGINAL] = false;
							break;
						case ConstDef::EVENT:
							$inputjson[ConstDef::TYPE] = ConstDef::EVENT;
							$inputjson[ConstDef::PRODUCT] = ConstDef::UDW;
							$inpujson[ConstDef::UDWTABLE] = $startnode->getTableName();
							break;
						case ConstDef::MV:
							$inputjson[ConstDef::TYPE] = ConstDef::MV;
							$inputjson[ConstDef::PRODUCT] = ConstDef::UDW;
							$inpujson[ConstDef::UDWTABLE] = $startnode->getTableName();
							break;
					}
					$xmlpath = null;
					$index = 0;
					foreach ($sflow->flowattr->xmlpath as $xmlpath) {
						$startnodename = $sflow->startnodes[0]->name;
						$endnodename = $sflow->endnodes[0]->name;
						$mappath = $startnodename . "_" . $endnodename . "_" . $index . "_xml";
						$inputjson[ConstDef::MAPPINGFILEPATH] = $mappath;
					}
					$mapperjson[ConstDef::MAPPERARGUMENTS][ConstDef::INPUTDATA][] = $inputjson;
				}
				foreach ($sflow->endnodes as $endnode) {
					$outputjson = array();
					$outputjson[ConstDef::NAME] = $endnode->name;
					switch ($endnode->type) {
						case ConstDef::TEMP:
							$outputjson[ConstDef::TYPE] = ConstDef::TEMP;
							break;
						case ConstDef::EVENT:
						case ConstDef::MV:
						case ConstDef::BIGEVENT:
							$outputjson[ConstDef::TYPE] = ConstDef::EVENT;
							$outputjson[ConstDef::UDWTABLE] = $startnode->getTableName();
							break;
					}		
				}
				if (count($sflow->startnodes) === 1) {
					$outputjson[ConstDef::FROM] = $sflow->startnodes[0]->name;
				} else {
					$outputjson[ConstDef::FROM] = ConstDef::ALL;
				}
				$mapperjson[ConstDef::MAPPERARGUMENTS][ConstDef::OUTPUTDATA][] = $outputjson;
				$index = 0;
				$dispxml = null;
				if ($sflow->flowattr->disp !== null || $sflow->flowattr->disp !== "") {
					$startnodename = $sflow->startnodes[0]->name;
					$endnodename = $sflow->endnodes[0]->name;
					$dispxml = $startnodename . "_" . $endnodename . "_" . $index . "_dispacher";
					$mapperjson[ConstDef::MAPPERARGUMENTS][ConstDef::MAPPERFUNCTION][ConstDef::DISPACHERFILEPATH] = $dispxml;
					$index++;
				}
			}
		}
		$mappercmd = "nohup echo " . json_encode($mapperjson) . " >{WORK_PATH}/mapper.json&";
		return $mappercmd;
	}
 };
 
  class ReducerJsonCommand extends BaseCommand {
 	public function getType() {
		return ConstDefs::LOCAL;
	}
	
	public function isNeed() {
		$ret = false;
		if ($this->sortedflows[$this->index][0]->type === ConstDef::JOIN) {
			$ret = true;
		} else {
			$ret = false;
		}
		return $ret;
	}
	
	public function excute($sortedflow) {
		$reducejson = array();
		foreach ($sortedflow as $sflow) {
			if ($sflow->type === ConstDef::JOIN) {
				foreach ($sflow->startnodes as $startnode) {
					$inputjson = array();
					$inputjson[ConstDef::NAME] = $startnode->name;
					$inputjson[ConstDef::TYPE] = ConstDef::TEMP;
					$nodename = null;
					foreach ($sortedflow as $stflow) {
						if (in_array($startnode->name, $stflow->endnodes)) {
							$nodename = $stflow->startnodes[0]->name;
						}
					}
					$inputjson[ConstDef::FROM] = $nodename;	
				}
				foreach ($sflow->endnodes as $endnode) {
					$outputjson = array();
					$outputjson[ConstDef::NAME] = $endnode->name;
					$outputjson[ConstDef::FROM] = ConstDef::ALL;
					$outputjson[ConstDef::UDWTABLE] = $endnode->getTableName();
				}
				$reducerjson[ConstDef::REDUCERARGUMENTS][INPUTDATA][] = $inputjson;
				$reducerjson[ConstDef::REDUCERARGUMENTS][OUTPUTDATA][] = $outputjson;
				$index = 0;
				foreach ($sflow->flowattr->xml as $xml) {
					$startnodename = $sflow->startnodes[0]->name;
					$endnodename = $sflow->endnodes[0]->name;
					$mappath = $startnodename . "_" . $endnodename . "_" . $index . "_xml";
					$reducerjson[ConstDef::REDUCERARGUMENTS][ConstDef::REDUCERFUNCTION][ConstDef::MAPPINGFILEPATH] = $mappath;
					$index++;
				}
				$index = 0;
				foreach ($sflow->flowattr->disp as $disp) {
					$startnodename = $sflow->startnodes[0]->name;
					$endnodename = $sflow->endnodes[0]->name;
					$disppath = $startnodename . "_" . $endnodename . "_" . $index . "_dispacher";
					$reducerjson[ConstDef::REDUCERARGUMENTS][ConstDef::REDUCERFUNCTION][ConstDef::DISPACHERFILEPATH] = $disppath;
					$index++;
				}
			}
		}
		$reducercmd = "nohup echo " . json_encode($reducerjson) . " >{WORK_PATH}/reducer.json&";
		return $reducercmd;
	}
 };
 
 class PapiJobInfoCommand extends BaseCommand {
 	public function getFiles() {
 		BaseCommand::files = null;
 	}
 	
 	public function getType() {
 		return ConstDef::LOCAL;
 	}
 	
 	public function excute($sortedflow) {
 		$papiinput = null;
 		$papioutput = null;
 		$outputtables = null;
 		$oversions = null;
 		foreach ($sortedflow as $sflow) {
 			foreach ($sflow->startnodes as $startnode) {
 				$isNeedRead = false;
 				if ($startnode->type === ConstDef::MV || $startnode->type === ConstDef::EVENT || $startnode->type === ConstDef::BIGEVENT) {
 					$isNeedRead = true;
 				} else {
 					continue;
 				}
 				$checkPoint = null;
				if ($dag->dagattr->freq == 1440) {
					$checkPoint = " -checkPoint event_day={DAY} ";
				} else if ($dag->dagattr->freq == 60) {
					$checkPoint = " -checkPoint event_day={DAY}.event_hour={HOUR} ";
				} else if ($dag->dagattr->freq < 60 && $dag->dagattr->freq > 0) {
					$checkPoint = " -checkPoint event_day={DAY}.event_hour={HOUR}.event_minute={MIN} ";
				}
				$papiinput = $papiinput . " -inputProj " . $startnode->getPapiTables() . $checkPoint;
 			}
 			foreach ($sflow->endnodes as $endnode) {
 				$isNeedWrite = false;
 				if (ConstDef::MV === $endnode->type || ConstDef::EVENT ===  $endnode->type || ConstDef::BIGEVENT === $endnode->type) {
					$isNeedWrite = true;
				} else {
				continue;
				}
				$oversion = null;
				if ($endnode->type === ConstDef::BIGEVENT) {
					$arr = explode('.', $endnode->name);
					if (count($arr) === 2) {
						$oversion = (string)$arr[1];
					} else {
						$oversion = "-1";
					}
				}
				$outputtables = $endnode->getPapiTables() . ",";
				$oversions = $oversion . ",";
 			}
 		}
 		$cmd = "{HADOOP_HOME}/../java6/bin/java -jar ";
 		$cmd = $cmd . "{LIB_JARS_PATH}/../lib.new/udw-program-api.jar GetJobInfo ";
 		$cmd = $cmd . "-server " . JobConfigs::ZKADDRESS . JobConfigs::ZKPATH . " ";
 		$cmd = $cmd . "-user InternalUser ";
 		$papioutput = " -output " . $outputtables . " -oversion " . $oversions;
 		if ($isNeedRead === true) {
 			$cmd = $cmd . $papiinput . " -ifile " . "{WORK_PATH}/input.schema.out" . " ";
 		}
 		if ($isNeedWrite === true) {
 			$cmd = $cmd . $papioutput . " -ofile {WORK_PATH}/output.schema.out ";
 		}
 	}
 	
 	public function isNeed() {
 		$ret = false;
 		if ($sortedflow[0]->type !== ConstDef::ANTISPAM) {
 			$ret = true;
 		}
 		return $ret;
 	}
 };
 
 class PapiCommitCommand extends BaseCommand {
 	public function getFiles() {
 		BaseCommand::files = null;
 	}
 	
 	public function getType() {
 		return ConstDef::LOCAL;
 	}
 	
 	public function isNeed() {
 		$ret = false;
 		if ($sortedflow[0]->type !== ConstDef::ANTISPAM) {
 			$ret = true;
 		}
 		return $ret;
 	}
 	
 	public function excute($sortedflow) {
 		$cmd = "{HADOOP_HOME}/../java6/bin/java -jar ";
		$cmd = $cmd . "{LIB_JARS_PATH}/../lib.new/udw-program-api.jar OutputCommit ";
		$cmd = $cmd . "-server " . JobConfigs::ZKADDRESS . JobConfigs::ZKPATH . " ";
		$cmd = $cmd . "-outputJobInfo {WORK_PATH}/output.schema.out ";
		$cmd = $cmd . "-outputDir \"/app/ns/udw/release/etl/etl_framework_temp/output{JOB_NAME}\" ";
		$cmd = $cmd . "-D fs.default.name=" + $dag->dagattr->fsname + " ";
 	}
 };