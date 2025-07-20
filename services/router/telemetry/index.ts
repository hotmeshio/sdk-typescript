import { TelemetryService } from '../../telemetry';
import { StreamData, StreamRole, StreamStatus } from '../../../types/stream';
import { HMSH_CODE_UNKNOWN } from '../config';

export class RouterTelemetry {
  private telemetryService: TelemetryService;

  constructor(appId: string) {
    this.telemetryService = new TelemetryService(appId);
  }

  startStreamSpan(input: StreamData, role: StreamRole): void {
    this.telemetryService.startStreamSpan(input, role);
  }

  setStreamError(error: string): void {
    this.telemetryService.setStreamError(error);
  }

  setStreamErrorFromOutput(output: any): void {
    if (output?.status === StreamStatus.ERROR) {
      this.telemetryService.setStreamError(
        `Function Status Code ${output.code || HMSH_CODE_UNKNOWN}`,
      );
    }
  }

  setStreamErrorFromException(err: Error): void {
    this.telemetryService.setStreamError(err.message);
  }

  setStreamAttributes(attributes: Record<string, any>): void {
    this.telemetryService.setStreamAttributes(attributes);
  }

  endStreamSpan(): void {
    this.telemetryService.endStreamSpan();
  }
}
