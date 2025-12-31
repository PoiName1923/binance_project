import asyncio
import logging
from aiokafka import AIOKafkaProducer
import websockets
from settings import settings
import json
# ===================================================================================================
# --- Cấu hình logging để theo dõi hoạt động của producer ---
logging.basicConfig(
    level=logging.INFO, # Mức log cơ bản là INFO
    format='%(asctime)s - %(levelname)s - %(message)s' # Định dạng của log message
)
# ===================================================================================================
class BinanceKafkaProducer:
    """
    Lớp xử lý việc thu thập dữ liệu giao dịch từ Binance WebSocket và gửi tới Kafka.
    
    Được thiết kế để đạt hiệu suất cao, sử dụng cơ chế bất đồng bộ (asynchronous).
    Tự động xử lý việc kết nối lại, tuân thủ các quy tắc của Binance,
    và giới hạn số lượng symbol trên mỗi tiến trình.
    """
    # Giới hạn số lượng cặp giao dịch tối đa mà một instance producer có thể xử lý
    MAX_SYMBOLS_PER_INSTANCE = 1000
    # URL cơ sở của Binance WebSocket Stream
    BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443/stream?streams="
# ===================================================================================================
    def __init__(self, symbol_file: str, kafka_topic: str, kafka_servers: str):
        """
        Hàm khởi tạo cho producer.

        Args:
            symbol_file (str): Đường dẫn tới file .txt chứa các cặp giao dịch.
            kafka_topic (str): Tên topic Kafka sẽ nhận dữ liệu.
            kafka_servers (str): Chuỗi chứa địa chỉ các server Kafka, cách nhau bởi dấu phẩy.
        """
        self.symbol_file = symbol_file
        self.kafka_topic = kafka_topic
        self.kafka_servers = kafka_servers
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Tải danh sách các cặp giao dịch từ file
        self.symbols = self._load_symbols()
        if not self.symbols:
            # Nếu không tải được symbol nào, dừng chương trình với lỗi
            raise ValueError("Không có symbol nào được tải. Vui lòng kiểm tra lại file symbol.")
            
        # Xây dựng URI để kết nối tới WebSocket
        self.ws_uri = self._build_ws_uri()
        # Khởi tạo producer là None, sẽ được tạo trong hàm `run` bất đồng bộ
        self.producer = None

# ===================================================================================================
    def _load_symbols(self) -> list[str]:
        """
        Tải danh sách các cặp giao dịch từ file, chuyển đổi sang chữ thường,
        và chỉ lấy tối đa số lượng cho phép (MAX_SYMBOLS_PER_INSTANCE).
        """
        try:
            with open(self.symbol_file, 'r') as f:
                # Đọc từng dòng, loại bỏ khoảng trắng thừa và chuyển sang chữ thường
                symbols = [line.strip().lower() for line in f if line.strip()]
            
            # Nếu số lượng symbol vượt quá giới hạn
            if len(symbols) > self.MAX_SYMBOLS_PER_INSTANCE:
                self.logger.warning(
                    f"Tìm thấy {len(symbols)} symbol, nhưng một instance chỉ có thể xử lý "
                    f"{self.MAX_SYMBOLS_PER_INSTANCE}. Chỉ lấy {self.MAX_SYMBOLS_PER_INSTANCE} symbol đầu tiên."
                )
                # Cắt danh sách để chỉ lấy số lượng tối đa cho phép
                symbols = symbols[:self.MAX_SYMBOLS_PER_INSTANCE]
            
            self.logger.info(f"Đã tải {len(symbols)} symbol từ file {self.symbol_file}.")
            return symbols
        except FileNotFoundError:
            self.logger.error(f"Không tìm thấy file symbol: {self.symbol_file}")
            return []
# ===================================================================================================
    def _build_ws_uri(self) -> str:
        """Xây dựng đường dẫn URI để kết nối tới Binance WebSocket."""
        # Mỗi stream có định dạng: <symbol>@trade
        streams = '/'.join([f"{symbol}@trade" for symbol in self.symbols])
        # Ghép các stream vào URL cơ sở
        return f"{self.BINANCE_WS_BASE_URL}{streams}"
    
# ===================================================================================================
    async def _connect_and_stream(self):
        """
        Kết nối tới WebSocket và bắt đầu luồng nhận dữ liệu để gửi tới Kafka.
        Hàm này xử lý logic chính trong một vòng đời kết nối.
        """
        # `websockets.connect` sẽ tự động xử lý ping/pong để giữ kết nối
        async with websockets.connect(self.ws_uri) as websocket:
            self.logger.info(f"Kết nối thành công tới Binance WebSocket cho {len(self.symbols)} luồng.")
            # Lặp qua từng tin nhắn nhận được từ WebSocket
            async for message in websocket:
                try:
                    # Tin nhắn từ Binance là một chuỗi JSON, phân tích nó thành dictionary
                    payload = json.loads(message)
                    
                    # Dữ liệu từ Binance có cấu trúc {"stream": "...", "data": {...}}
                    # Ta chỉ cần lấy đối tượng trong trường "data"
                    trade_data = payload.get('data')

                    if trade_data:
                        # Chuyển đổi lại đối tượng 'data' thành chuỗi JSON để gửi đi
                        value_to_send = json.dumps(trade_data)
                        self.logger.debug(f"Dữ liệu giao dịch được trích xuất: {value_to_send}")

                        # Gửi dữ liệu giao dịch đã được trích xuất (dưới dạng bytes) tới Kafka
                        await self.producer.send_and_wait(
                            self.kafka_topic,
                            value_to_send.encode('utf-8')
                        )
                        self.logger.info(value_to_send)
                    else:
                        # Ghi log nếu tin nhắn không có trường 'data' như mong đợi
                        self.logger.warning(f"Tin nhắn nhận được không có trường 'data': {message}")
 
                except json.JSONDecodeError:
                    # Bắt lỗi nếu tin nhắn nhận được không phải là một chuỗi JSON hợp lệ
                    self.logger.error(f"Không thể phân tích JSON từ tin nhắn: {message}")
                except Exception as e:
                    self.logger.error(f"Lỗi khi xử lý hoặc gửi tin nhắn: {e}")
# ===================================================================================================
    async def run(self):
        """
        Hàm chính để chạy producer. 
        Khởi tạo AIOKafkaProducer và chạy vòng lặp stream với cơ chế tự động kết nối lại.
        """
        # Khởi tạo Kafka producer bất đồng bộ
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_servers)
        # Bắt đầu producer
        await self.producer.start()
        self.logger.info("AIOKafkaProducer đã khởi động.")
        
        # Vòng lặp vô hạn để đảm bảo producer luôn chạy và tự động kết nối lại khi cần
        while True:
            try:
                # Bắt đầu quá trình kết nối và nhận dữ liệu
                await self._connect_and_stream()
            except websockets.exceptions.ConnectionClosed as e:
                # Khi kết nối bị đóng (ví dụ: sau 24 giờ), ghi log và thử kết nối lại ngay lập tức
                # để giảm thiểu thời gian mất dữ liệu.
                self.logger.warning(f"Kết nối WebSocket đã đóng: {e}. Đang kết nối lại ngay lập tức...")
            except Exception as e:
                # Đối với các lỗi không mong muốn khác (mất mạng, lỗi logic),
                # ghi log và chờ 1 giây trước khi thử lại để tránh spam.
                self.logger.error(f"Đã xảy ra lỗi không mong muốn: {e}. Đang kết nối lại sau 1 giây...")
                await asyncio.sleep(1)

    async def stop(self):
        """Dừng Kafka producer một cách an toàn."""
        if self.producer:
            await self.producer.stop()
            self.logger.info("AIOKafkaProducer đã dừng.")
# ===================================================================================================
# ===================================================================================================

def main():
    """Hàm chính để chạy producer từ dòng lệnh."""

    # Khởi tạo lớp producer với các tham số đã nhận
    producer = BinanceKafkaProducer(
        symbol_file=settings.SYMBOL_FILE_DIR,
        kafka_topic=settings.KAFKA_TOPIC,
        kafka_servers=f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"
    )

    try:
        # Chạy hàm bất đồng bộ `run`
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        # Xử lý khi người dùng nhấn Ctrl+C để dừng chương trình
        print("Yêu cầu tắt. Đang dừng producer...")
        asyncio.run(producer.stop())
    except ValueError as e:
        # Xử lý lỗi khởi tạo (ví dụ: không tìm thấy file symbol)
        logging.error(f"Khởi tạo thất bại: {e}")


# Điểm bắt đầu của chương trình khi được chạy trực tiếp từ terminal
if __name__ == "__main__":
    main()