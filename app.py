import os
from aiohttp import web
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
    MemoryStorage,
    ConversationState,
)
from botbuilder.schema import Activity, ActivityTypes, Attachment

import requests
import pandas as pd
import tempfile
import io
import base64

# Set up
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")

adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

memory = MemoryStorage()
conversation_state = ConversationState(memory)

# Helper: generate downloadable attachment
def create_excel_attachment(df, filename):
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    content_bytes = output.read()
    b64_content = base64.b64encode(content_bytes).decode()
    return Attachment(
        name=filename,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        content_url=f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_content}",
    )

# --- BOT LOGIC ---
async def messages(request):
    try:
        body = await request.json()
        activity = Activity().deserialize(body)
        auth_header = request.headers.get("Authorization", "")
        state = conversation_state.create_property("file_state")

        async def aux_func(turn_context: TurnContext):
            conversation_data = await state.get(turn_context)
            if conversation_data is None:
                conversation_data = {}

            # If user sends file
            attachments = turn_context.activity.attachments
            if attachments and len(attachments) > 0:
                file_info = attachments[0]
                file_name = file_info.name
                file_url = file_info.content_url

                try:
                    print(f"Downloading file from URL: {file_url}")
                    file_bytes = requests.get(file_url).content
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                        tmp_file.write(file_bytes)
                        tmp_file_path = tmp_file.name
                    print(f"Saved temp file to: {tmp_file_path}")

                    try:
                        df = pd.read_excel(tmp_file_path, header=None)
                        print(f"Read Excel file with shape: {df.shape}")
                    except Exception as e:
                        print("Error reading Excel file:", e)
                        await turn_context.send_activity(f"❌ Error reading Excel file: {str(e)}")
                        return

                    conversation_data["last_file_path"] = tmp_file_path
                    conversation_data["last_file_name"] = file_name
                    await state.set(turn_context, conversation_data)
                    await conversation_state.save_changes(turn_context)
                    await turn_context.send_activity(
                        f"✅ Received file: **{file_name}**\nNumber of rows: {df.shape[0]}\n\n"
                        f"Please enter the header row number (starting from 0), e.g.: `Header row: 4`"
                    )
                except Exception as e:
                    print("Exception during file handling:", e)
                    await turn_context.send_activity(f"❌ Failed to process file: {str(e)}")
                return

            # If user sends the header row info
            if turn_context.activity.text and "header row:" in turn_context.activity.text.lower():
                try:
                    header_row = int(turn_context.activity.text.split(":")[1].strip())
                except Exception:
                    await turn_context.send_activity("Please enter header row in format: `Header row: 4`")
                    return

                if "last_file_path" in conversation_data:
                    tmp_file_path = conversation_data["last_file_path"]
                    file_name = conversation_data["last_file_name"]
                    try:
                        clean_df = pd.read_excel(tmp_file_path, header=header_row)
                        cleaned_filename = "cleaned_" + file_name
                        attachment = create_excel_attachment(clean_df, cleaned_filename)
                        await turn_context.send_activity(
                            f"🧹 Cleaned file is ready. Download below:",
                            attachments=[attachment]
                        )
                    except Exception as e:
                        print("Error cleaning Excel file:", e)
                        await turn_context.send_activity(f"❌ Failed to clean/process file: {str(e)}")
                else:
                    await turn_context.send_activity("Please send an Excel file first.")
                return

            # Else: help
            await turn_context.send_activity(
                "Send me an Excel (.xlsx) file. Then send the header row (e.g., `Header row: 4`)."
            )

        await adapter.process_activity(activity, auth_header, aux_func)
        return web.Response(status=200)
    except Exception as e:
        print(f"Global error: {e}")
        return web.Response(status=500, text=str(e))

async def home(request):
    return web.Response(text="<h2>Bot is running!</h2>", content_type="text/html")

async def messages_get(request):
    return web.Response(
        text="This endpoint is for POST requests only. Please POST your bot message payload here.",
        content_type="text/plain",
        status=405
    )

app = web.Application()
app.router.add_get("/", home)
app.router.add_post("/api/messages", messages)
app.router.add_get("/api/messages", messages_get)

if __name__ == "__main__":
    web.run_app(app, host="localhost", port=3978)
