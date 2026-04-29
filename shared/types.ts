export interface CheckTask {
  url: string;
  id: string;
  timestamp: number;
}

export interface CheckResult {
  id: string;
  url: string;
  status: 'up' | 'down';
  statusCode?: number;
  responseTime: number;
}