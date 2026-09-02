import type { StandardRequestInputMessage } from '../../types';

export type StandardImageReference =
  | {
      type: 'base64';
      mediaType: string;
      data: string;
    }
  | {
      type: 'url';
      url: string;
    };

export function buildStandardImageDataUrl(data: unknown, mediaType: unknown): string | undefined {
  if (typeof data !== 'string' || !data.trim()) {
    return undefined;
  }

  const normalizedMediaType =
    mediaType === undefined || mediaType === null || mediaType === ''
      ? 'image/png'
      : normalizeImageMediaType(mediaType);
  if (!normalizedMediaType) {
    return undefined;
  }

  return `data:${normalizedMediaType};base64,${data.trim()}`;
}

export function parseStandardImageReference(value: string): StandardImageReference | undefined {
  const normalized = value.trim();
  if (/^https?:\/\//i.test(normalized)) {
    return {
      type: 'url',
      url: normalized
    };
  }

  if (!normalized.toLowerCase().startsWith('data:')) {
    return undefined;
  }

  const separatorIndex = normalized.indexOf(',');
  if (separatorIndex < 0) {
    return undefined;
  }

  const metadata = normalized.slice(5, separatorIndex).split(';');
  const mediaType = normalizeImageMediaType(metadata[0]);
  const isBase64 = metadata.slice(1).some((item) => item.trim().toLowerCase() === 'base64');
  const data = normalized.slice(separatorIndex + 1).trim();
  if (!mediaType || !isBase64 || !data) {
    return undefined;
  }

  return {
    type: 'base64',
    mediaType,
    data
  };
}

export function findInvalidStandardImageInput(
  input: string | StandardRequestInputMessage[]
): string | undefined {
  if (typeof input === 'string') {
    return undefined;
  }

  for (const message of input) {
    for (const item of message.content) {
      if (item.type === 'input_image' && !parseStandardImageReference(item.image_url)) {
        return item.image_url;
      }
    }
  }

  return undefined;
}

function normalizeImageMediaType(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  return normalized.startsWith('image/') ? normalized : undefined;
}
