<?php
/**
 * Class DatabaseMaintainer
 * 
 * 
 * @package DatabaseMaintainer
 * @version 1.0.0
 * @since 2024
 * @license MIT
 * 
 * COPYRIGHT: 2024 Fred Onis - All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * @author Fred Onis
 */
class DatabaseMaintainer {
    private $db;
    private $dbConfigPath;
    private $log;
	private $tables;
    private $timeStart;

    /**
     * GiataDefinitionsImporter constructor.
     * 
     * @param Database $db The database connection object.
     * @param string $url The URL to fetch JSON data from.
     */
    public function __construct($dbConfigPath) {
		$this->dbConfigPath  = $dbConfigPath;
        $this->log = new Log();
        $this->registerExitHandler();
		$this->connectDatabase();
    }

    /**
     * Register the exit handler.
     *
     * @return void
     */
    private function registerExitHandler() {
        $this->timeStart = microtime(true);
        register_shutdown_function([new ExitHandler($this->timeStart), 'handleExit']);
    }

	/**
	 * Connects to the database using the configuration file.
	 *
	 * This method reads the database configuration from the specified INI file,
	 * parses the configuration, and establishes a connection to the database.
	 * If the configuration file cannot be parsed, an exception is thrown.
	 *
	 * @throws Exception If the configuration file cannot be parsed.
	 * @return void
	 */
	private function connectDatabase() {
		if (($dbConfig = parse_ini_file($this->dbConfigPath, FALSE, INI_SCANNER_TYPED)) === FALSE) {
			throw new Exception("Parsing file " . $this->dbConfigPath	. " FAILED");
		}
		$this->db = new Database($dbConfig);
	}

    public function maintenance() {
        $this->tables = $this->getTables();
		$this->repairTableCorruption();
		$this->updateIndexStatistics();
		$this->reduceFragmentation();
    }

    private function getTables() {
		$query			=	"
		SELECT			table_schema, 
						table_name
		FROM			information_schema.tables 
		WHERE			table_schema	<>	'information_schema'
		ORDER BY		1";
		return	$this->db->select($query);
    }
	
	private function repairTableCorruption() {
		$this->log->info('Finding and Repairing Table Corruption');
		foreach ($this->tables as $key => $array) {
			$this->db->execute('CHECK TABLE ' . $array['table_schema'] . '.' . $array['table_name']);
		}
	}
	
	private function updateIndexStatistics() {
		$this->log->info('Updating Index Statistics');
		foreach ($this->tables as $key => $array) {
			$this->db->execute('ANALYZE TABLE ' . $array['table_schema'] . '.' . $array['table_name']);
		}
	}
	
	private function reduceFragmentation() {
		$this->log->info('Reducing Index and Data Fragmentation');
		foreach ($this->tables as $key => $array) {
			$this->db->execute('OPTIMIZE TABLE ' . $array['table_schema'] . '.' . $array['table_name']);
		}
	}
}