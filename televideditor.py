import os
import time
import requests
import json
import logging
import base64
import subprocess
from PIL import Image, ImageDraw, ImageFont, ImageFilter # <-- Add ImageFilter
import textwrap

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- Constants and Configuration ---
# --- Constants and Configuration ---
BOT_TOKEN_2 = os.environ.get("BOT_TOKEN")
WORKER_PUBLIC_URL = os.environ.get("WORKER_PUBLIC_URL")
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN")  # <-- Add this
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID")  # <-- Add this
# Add these to your Constants section
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not all([BOT_TOKEN_2, WORKER_PUBLIC_URL, RAILWAY_API_TOKEN, RAILWAY_SERVICE_ID]):
    raise ValueError("BOT_TOKEN_2, WORKER_PUBLIC_URL, RAILWAY_API_TOKEN, and RAILWAY_SERVICE_ID environment variables must be set!")

# --- Video Processing Constants ---
COMP_WIDTH = 1080
COMP_HEIGHT = 1920
COMP_SIZE_STR = f"{COMP_WIDTH}x{COMP_HEIGHT}"
BACKGROUND_COLOR = "black"
FPS = 30
IMAGE_DURATION = 12
MEDIA_FADE_DURATION = 3      # <-- ADD THIS: Duration for the background media fade
CAPTION_FADE_DURATION = 11    # <-- ADD THIS: Duration for the caption fade
MEDIA_Y_OFFSET = 0
CAPTION_V_PADDING = 37
CAPTION_FONT_SIZE = 40
CAPTION_TOP_PADDING_LINES = 0
CAPTION_LINE_SPACING = 5
CAPTION_FONT = "ZalandoSans-Medium"
CAPTION_TEXT_COLOR = (255, 255, 255) # <-- CHANGED to white
CAPTION_BG_COLOR = (255, 255, 255) # <-- No longer used, but kept
# NEW CONSTANTS FOR TEXT SHADOW
SHADOW_COLOR = (0, 0, 0)         # Solid black is best for blurring
SHADOW_OFFSET = (0, 0)           # (x, y) offset in pixels for the shadow
SHADOW_BLUR_RADIUS = 20           # <-- NEW: Controls blurriness. Higher = softer/more spread.

# --- File Paths ---
DOWNLOAD_PATH = "downloads"
OUTPUT_PATH = "outputs"

# --- Helper Functions ---

def cleanup_files(file_list):
    """Safely delete a list of files."""
    for file_path in file_list:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up file: {file_path}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")

def create_directories():
    """Create necessary directories if they don't exist."""
    for path in [DOWNLOAD_PATH, OUTPUT_PATH]:
        if not os.path.exists(path):
            os.makedirs(path)

# --- Railway API Functions ---

def stop_railway_deployment():
    """
    Stops the Railway deployment by fetching the latest deployment ID
    and using the Railway GraphQL API to trigger a stop on that specific deployment.
    """
    logging.info("Attempting to stop Railway deployment...")
    api_token = os.environ.get("RAILWAY_API_TOKEN")
    service_id = os.environ.get("RAILWAY_SERVICE_ID")

    if not api_token or not service_id:
        logging.warning("RAILWAY_API_TOKEN or RAILWAY_SERVICE_ID is not set. Skipping stop command.")
        return

    graphql_url = "https://backboard.railway.app/graphql/v2"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    # Step 1: Get the latest deployment ID (This is identical to the restart logic)
    get_id_query = {
        "query": """
            query getLatestDeployment($serviceId: String!) {
                service(id: $serviceId) {
                    deployments(first: 1) {
                        edges {
                            node { id }
                        }
                    }
                }
            }
        """,
        "variables": {"serviceId": service_id}
    }

    try:
        response = requests.post(graphql_url, json=get_id_query, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        deployment_id = data['data']['service']['deployments']['edges'][0]['node']['id']
        logging.info(f"Successfully fetched latest deployment ID for shutdown: {deployment_id}")

    except (requests.exceptions.RequestException, KeyError, IndexError) as e:
        logging.error(f"Failed to get Railway deployment ID for shutdown: {e}")
        if 'response' in locals():
            logging.error(f"Response from Railway: {response.text}")
        return # Stop if we can't get the ID

    # Step 2: Trigger the stop using the fetched deployment ID
    stop_mutation = {
        "query": """
            mutation deploymentStop($id: String!) {
                deploymentStop(id: $id)
            }
        """,
        "variables": {"id": deployment_id}
    }

    try:
        response = requests.post(graphql_url, json=stop_mutation, headers=headers, timeout=15)
        response.raise_for_status()
        logging.info("Successfully sent stop command to Railway. Service will shut down.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send stop command to Railway: {e}")
        if 'response' in locals():
            logging.error(f"Response from Railway: {response.text}")
# --- Worker Communication Functions ---

# In televideditor.py

def fetch_job_from_redis():
    """Fetches a single job from the Upstash Redis queue endpoint."""
    # RPOP atomically reads and removes the last element from the list.
    url = f"{UPSTASH_REDIS_REST_URL}/rpop/job_queue"
    headers = {
        "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"
    }

    # --- THIS IS THE CORRECTED BLOCK ---
    try:
        response = requests.get(url, headers=headers, timeout=5)  # 5s timeout is plenty
        response.raise_for_status()
        
        data = response.json()
        result = data.get("result")

        if result:
            logging.info("Successfully fetched a new job from Redis.")
            # The result is a JSON string, so we need to parse it back into a Python dictionary
            return json.loads(result)
        else:
            # If the list is empty, Upstash returns a null result, so we return None.
            logging.info("Job queue in Redis is empty.")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Could not connect to Redis to fetch job: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from Redis response: {e}")
        logging.error(f"Raw response from Redis: {response.text if 'response' in locals() else 'No response'}")
        return None

def submit_result_to_worker(chat_id, video_path, frame_path):
    """Uploads the final video and a frame to the worker."""
    url = f"{WORKER_PUBLIC_URL}/submit-result"
    logging.info(f"Submitting result for chat_id {chat_id} to worker...")
    try:
        with open(frame_path, "rb") as image_file, open(video_path, 'rb') as video_file:
            image_data = base64.b64encode(image_file.read()).decode('utf-8')
            
            # <-- NEW DEBUG LOG
            # This confirms that the frame data is not empty before sending.
            logging.info(f"Frame data prepared for upload. Size: {len(image_data)} characters.")

            files = {
                'video': ('final_video.mp4', video_file, 'video/mp4'),
                'image_data': (None, image_data),
                'chat_id': (None, str(chat_id))
            }
            response = requests.post(url, files=files, timeout=60)
            response.raise_for_status()
        logging.info("Successfully submitted result to worker.")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error uploading result to worker: {e}")
        if 'response' in locals():
            logging.error(f"Worker response: {response.text}") # <-- MORE DETAIL ON ERROR
        return False

# --- Core Processing Logic (Adapted from original script) ---

def download_telegram_file(file_id, job_id):
    """Downloads a file from Telegram using a file_id."""
    try:
        # Step 1: Get file path from Telegram API
        file_info_url = f"https://api.telegram.org/bot{BOT_TOKEN_2}/getFile"
        params = {'file_id': file_id}
        response = requests.get(file_info_url, params=params, timeout=15)
        response.raise_for_status()
        file_info = response.json()['result']
        
        file_path = file_info['file_path']
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN_2}/{file_path}"
        file_extension = os.path.splitext(file_path)[1]
        save_path = os.path.join(DOWNLOAD_PATH, f"{job_id}{file_extension}")

        # Step 2: Stream the file download
        with requests.get(file_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        logging.info(f"Successfully downloaded media to {save_path}")
        return save_path

    except Exception as e:
        logging.error(f"Failed to download file_id {file_id}: {e}", exc_info=True)
        return None

def get_media_dimensions(media_path, media_type):
    # This function remains the same as the original
    if media_type == 'image':
        with Image.open(media_path) as img:
            return img.width, img.height, IMAGE_DURATION
    else: # video
        command = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height,duration', '-of', 'json', media_path]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
            data = json.loads(result.stdout)['streams'][0]
            return data['width'], data['height'], float(data['duration'])
        except Exception as e:
            logging.error(f"FFprobe failed: {e}")
            return None, None, None

def create_caption_image(text, job_id):
    """
    Creates a caption image with white, stroked text and a soft, blurred drop shadow
    on a transparent background.
    """
    padded_text = ("\n" * CAPTION_TOP_PADDING_LINES) + text
    font_path = f"{CAPTION_FONT}.ttf"
    font = ImageFont.truetype(font_path, CAPTION_FONT_SIZE)
    
    final_lines = [item for line in padded_text.split('\n') for item in textwrap.wrap(line, width=30, break_long_words=True) or ['']]
    wrapped_text = "\n".join(final_lines)
    
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0,0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), wrapped_text, font=font, align="center", spacing=CAPTION_LINE_SPACING, stroke_width=1)
    
    text_width = int(text_bbox[2] - text_bbox[0])
    text_height = int(text_bbox[3] - text_bbox[1])
    
    # Increase image padding to prevent the blur from getting cut off at the edges
    img_padding = SHADOW_BLUR_RADIUS * 4
    img_width = text_width + img_padding
    img_height = text_height + img_padding

    # --- New Shadow and Blur Logic ---

    # 1. Create a temporary image for the shadow layer
    shadow_img = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_img)

    # 2. Draw the shadow source text onto the temporary image
    # We add the blur radius to the position to keep it centered after padding
    shadow_pos = (
        img_padding / 2 + SHADOW_OFFSET[0],
        img_padding / 2 + SHADOW_OFFSET[1]
    )
    shadow_draw.multiline_text(
        shadow_pos,
        wrapped_text,
        font=font,
        fill=SHADOW_COLOR,
        anchor="la", # Use left-align anchor for precise positioning
        align="center",
        spacing=CAPTION_LINE_SPACING,
        stroke_width=2,
        stroke_fill=SHADOW_COLOR
    )

    # 3. Blur the entire shadow layer
    shadow_img = shadow_img.filter(ImageFilter.GaussianBlur(radius=SHADOW_BLUR_RADIUS))

    # --- Main Text Drawing ---
    
    # 4. Create the final image (we'll paste the shadow and draw the text on this)
    # The final image is the blurred shadow image itself.
    final_draw = ImageDraw.Draw(shadow_img)

    # 5. Draw the main, sharp text on top of the blurred shadow
    text_pos = (img_padding / 2, img_padding / 2)
    final_draw.multiline_text(
        text_pos,
        wrapped_text,
        font=font,
        fill=CAPTION_TEXT_COLOR,
        anchor="la", # Use left-align anchor
        align="center",
        spacing=CAPTION_LINE_SPACING,
        stroke_width=2,
        stroke_fill=(0, 0, 0)
    )
    
    caption_image_path = os.path.join(OUTPUT_PATH, f"caption_{job_id}.png")
    shadow_img.save(caption_image_path)
    
    return caption_image_path, text_height
    
def extract_frame_from_video(video_path, duration, job_id):
    """Extract a frame from the midpoint of a video."""
    frame_path = os.path.join(OUTPUT_PATH, f"frame_{job_id}.jpg")
    midpoint = duration / 2
    command = [
        'ffmpeg', '-y', '-i', video_path, '-ss', str(midpoint),
        '-vframes', '1', frame_path
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
        # <-- NEW DEBUG LOG
        logging.info(f"Successfully extracted frame for job {job_id} to {frame_path}")
        return frame_path
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logging.error(f"FFmpeg frame extraction failed: {getattr(e, 'stderr', e)}")
        return None

def process_video_job(job_data):
    """The main video creation logic for a single job."""
    chat_id = job_data['chat_id']
    job_id = job_data['job_id']
    logging.info(f"Starting processing for job_id: {job_id}")

    files_to_clean = []
    
    try:
        # Steps 1-3 remain the same
        media_path = download_telegram_file(job_data['file_id'], job_id)
        if not media_path: raise ValueError("Media download failed.")
        files_to_clean.append(media_path)

        media_type = job_data['media_type']
        media_w, media_h, final_duration = get_media_dimensions(media_path, media_type)
        if not all([media_w, media_h, final_duration]): raise ValueError("Could not get media dimensions.")

        caption_image_path, _ = create_caption_image(job_data['caption_text'], job_id)
        files_to_clean.append(caption_image_path)

        # 4. Run FFmpeg
        output_filepath = os.path.join(OUTPUT_PATH, f"output_{job_id}.mp4")
        
        scale_ratio = COMP_WIDTH / media_w
        scaled_media_h = int(media_h * scale_ratio)
        media_y_pos = int((COMP_HEIGHT / 2 - scaled_media_h / 2) + MEDIA_Y_OFFSET)

        command = ['ffmpeg', '-y', '-f', 'lavfi', '-i', f'color=c={BACKGROUND_COLOR}:s={COMP_SIZE_STR}:d={final_duration}']
        if media_type == 'image':
            command.extend(['-loop', '1', '-t', str(final_duration)])
        command.extend(['-i', media_path, '-i', caption_image_path])

        # --- CORRECTED SEQUENTIAL FADE LOGIC ---
        
        # 1. Create the base scene with the media placed on the background
        filter_parts = [
            f"[1:v]scale={COMP_WIDTH}:-1,setpts=PTS-STARTPTS[scaled_media]",
            f"[0:v][scaled_media]overlay=(W-w)/2:{media_y_pos}[base_scene]"
        ]
        
        # Use a variable to track the current state of the video stream
        current_scene_label = "base_scene"
        final_output_label = "final_v"

        if job_data['apply_fade']:
            # 2. Apply the FIRST fade layer (for the media) on top of the base scene
            filter_parts.extend([
                f"color=c=black:s={COMP_SIZE_STR}:d={final_duration},format=rgba,fade=t=out:st=0:d={MEDIA_FADE_DURATION}[media_fade_layer]",
                f"[{current_scene_label}][media_fade_layer]overlay=0:0[scene_after_media_fade]"
            ])
            current_scene_label = "scene_after_media_fade" # Update our current scene

            # 3. Overlay the TEXT on top of the already-fading scene
            filter_parts.append(
                f"[{current_scene_label}][2:v]overlay=(W-w)/2:(H-h)/2[scene_with_text]"
            )
            current_scene_label = "scene_with_text" # Update again

            # 4. Apply the SECOND fade layer (for the caption) on top of EVERYTHING
            filter_parts.extend([
                f"color=c=black:s={COMP_SIZE_STR}:d={final_duration},format=rgba,fade=t=out:st=0:d={CAPTION_FADE_DURATION}[caption_fade_layer]",
                f"[{current_scene_label}][caption_fade_layer]overlay=0:0[{final_output_label}]"
            ])
        else:
            # If no fade, just overlay the text and we're done
            filter_parts.append(
                f"[{current_scene_label}][2:v]overlay=(W-w)/2:(H-h)/2[{final_output_label}]"
            )

        # --- END OF FILTER LOGIC ---
        
        filter_complex = ";".join(filter_parts)
        map_args = ['-map', f'[{final_output_label}]']
        if media_type == 'video':
            filter_complex += ";[1:a]asetpts=PTS-STARTPTS[final_a]"
            map_args.extend(['-map', '[final_a]'])
        
        command.extend([
            '-filter_complex', filter_complex, *map_args,
            '-c:v', 'libx264',
            '-preset', 'ultrafast', '-tune', 'zerolatency',
            '-c:a', 'aac', '-b:a', '192k',
            '-r', str(FPS),
            '-pix_fmt', 'yuv420p',
            output_filepath
        ])
        
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logging.error(f"FFMPEG STDERR: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
        
        logging.info(f"FFmpeg processing finished for job {job_id}.")
        files_to_clean.append(output_filepath)

        # Steps 5 & 6 remain the same
        frame_path = extract_frame_from_video(output_filepath, final_duration, job_id)
        if not frame_path: raise ValueError("Frame extraction failed.")
        files_to_clean.append(frame_path)
        submit_result_to_worker(chat_id, output_filepath, frame_path)

    except Exception as e:
        error_snippet = str(e)[-1000:]
        logging.error(f"Failed to process job {job_id}: {error_snippet}", exc_info=True)
    
    finally:
        logging.info(f"Cleaning up files for job {job_id}.")
        cleanup_files(files_to_clean)

# In televideditor.py

# --- Main Bot Loop ---
if __name__ == '__main__':
    logging.info("Starting Python Job Processor...")
    create_directories()
    
    start_time = time.time()
    timeout_seconds = 60  # Wait a maximum of 60 seconds for the first job
    first_job = None

    # --- Polling Loop with Timeout ---
    # This loop runs for up to 60 seconds, waiting for the user to finish their choices.
    logging.info(f"Pre-warmed. Polling for first job for up to {timeout_seconds} seconds...")
    while time.time() - start_time < timeout_seconds:
        job = fetch_job_from_redis()
        if job:
            logging.info("First job found in queue. Starting processing.")
            first_job = job
            break  # Exit the polling loop
        
        # Wait for 1 second before polling again
        time.sleep(3)
    
    # --- Processing Phase ---
    if first_job:
        # If we found a job, process it and then continue to process any other jobs in the queue.
        process_video_job(first_job)
        
        # Now, process the rest of the queue without a timeout
        while True:
            job = fetch_job_from_redis()
            if job:
                process_video_job(job)
            else:
                # The queue is now empty
                logging.info("Job queue is empty.")
                break # Exit the queue-processing loop
    else:
        # If the polling loop finished without finding any job, it means the user abandoned the process.
        logging.warning(f"No job found within the {timeout_seconds} second timeout. Shutting down to conserve resources.")

    # --- Shutdown Phase ---
    # This code runs whether we processed jobs or timed out.
    logging.info("All tasks complete or timed out. Requesting shutdown.")
    stop_railway_deployment()
    logging.info("Processor has finished its work and is exiting.")
