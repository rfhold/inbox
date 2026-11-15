const paths = {
  mail: 'M3 5h18v14H3z M3 5l9 7 9-7',
  inbox: 'M4 4h16l2 12v4H2v-4L4 4z M2 15h6l2 3h4l2-3h6',
  sent: 'M22 2L9 15 M22 2l-7 20-6-7-7-6L22 2z',
  draft: 'M14 2H5v20h14V7l-5-5z M14 2v6h5 M8 13h8 M8 17h6',
  compose: 'M16 3l5 5-12 12H4v-5L16 3z M13 6l5 5',
  back: 'M19 12H5 M11 6l-6 6 6 6',
  refresh: 'M20 7a9 9 0 10.5 9 M20 2v6h-6',
  plus: 'M12 5v14 M5 12h14',
  reply: 'M9 5l-7 7 7 7 M2 12h11a8 8 0 018 8',
  read: 'M3 5h18v14H3z M3 5l9 7 9-7',
  download: 'M12 3v12 M7 10l5 5 5-5 M4 16v5h16v-5',
} as const;

export function Icon(props: {name: keyof typeof paths}) {
  return <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={paths[props.name]} /></svg>;
}
